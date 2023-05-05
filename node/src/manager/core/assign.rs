use graph::prelude::{anyhow::anyhow, Error, NodeId, StoreEvent};
use graph_store_postgres::{
    command_support::catalog, connection_pool::ConnectionPool, NotificationSender,
};
use std::thread;
use std::time::Duration;

use crate::manager::deployment::DeploymentSearch;

pub fn unassign(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
) -> Result<(), Error> {
    let locator = search.locate_unique(&primary)?;

    let conn = primary.get()?;
    let conn = catalog::Connection::new(conn);

    let site = conn
        .locate_site(locator.clone())?
        .ok_or_else(|| anyhow!("failed to locate site for {locator}"))?;

    let changes = conn.unassign_subgraph(&site)?;
    conn.send_store_event(sender, &StoreEvent::new(changes))?;

    Ok(())
}

pub fn reassign(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
    node: String,
) -> Result<Option<NodeId>, Error> {
    let node = NodeId::new(node.clone()).map_err(|()| anyhow!("illegal node id `{}`", node))?;
    let locator = search.locate_unique(&primary)?;

    let conn = primary.get()?;
    let conn = catalog::Connection::new(conn);

    let site = conn
        .locate_site(locator.clone())?
        .ok_or_else(|| anyhow!("failed to locate site for {locator}"))?;
    let changes = match conn.assigned_node(&site)? {
        Some(cur) => {
            if cur == node {
                vec![]
            } else {
                conn.reassign_subgraph(&site, &node)?
            }
        }
        None => conn.assign_subgraph(&site, &node)?,
    };
    conn.send_store_event(sender, &StoreEvent::new(changes))?;

    // Return the assigned node for further processing
    Ok(conn.assigned_node(&site)?)
}

pub fn pause_or_resume(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
    should_pause: bool,
) -> Result<(), Error> {
    let locator = search.locate_unique(&primary)?;

    let conn = primary.get()?;
    let conn = catalog::Connection::new(conn);

    let site = conn
        .locate_site(locator.clone())?
        .ok_or_else(|| anyhow!("failed to locate site for {locator}"))?;

    let change = match conn.assignment_status(&site)? {
        Some((_, is_paused)) => {
            if should_pause {
                if is_paused {
                    println!("deployment {locator} is already paused");
                    return Ok(());
                }
                println!("pausing {locator}");
                conn.pause_subgraph(&site)?
            } else {
                println!("resuming {locator}");
                conn.resume_subgraph(&site)?
            }
        }
        None => {
            println!("deployment {locator} not found");
            return Ok(());
        }
    };
    println!("Operation completed");
    conn.send_store_event(sender, &StoreEvent::new(change))?;

    Ok(())
}

pub fn restart(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
    sleep: Duration,
) -> Result<(), Error> {
    pause_or_resume(primary.clone(), sender, search, true)?;
    println!(
        "Waiting {}s to make sure pausing was processed",
        sleep.as_secs()
    );
    thread::sleep(sleep);
    pause_or_resume(primary, sender, search, false)?;
    Ok(())
}
