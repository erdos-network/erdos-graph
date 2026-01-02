#![feature(coverage_attribute)]

pub mod config;
pub mod db;
pub mod logger;
pub mod schedulers;
pub mod scrapers;
pub mod utilities;

#[cfg(test)]
pub mod tests;
