use crate::maxminddb_crate;
use pyo3::{
    prelude::*,
    types::{PyDict, PyList},
};
use std::collections::BTreeMap;

/// Metadata about the MaxMind DB database.
#[pyclass(module = "maxminddb_rust")]
pub(crate) struct Metadata {
    /// The major version number of the binary format used when creating the database.
    #[pyo3(get)]
    binary_format_major_version: u16,
    /// The minor version number of the binary format used when creating the database.
    #[pyo3(get)]
    binary_format_minor_version: u16,
    /// The Unix epoch timestamp for when the database was built.
    #[pyo3(get)]
    build_epoch: u64,
    /// A string identifying the database type (e.g., 'GeoIP2-City', 'GeoLite2-Country').
    #[pyo3(get)]
    database_type: String,
    description_dict: BTreeMap<String, String>,
    /// The IP version of the data in a database. A value of 4 means IPv4 only; 6 supports both IPv4 and IPv6.
    #[pyo3(get)]
    ip_version: u16,
    languages_list: Vec<String>,
    /// The number of nodes in the search tree.
    #[pyo3(get)]
    node_count: u32,
    /// The record size in bits (24, 28, or 32).
    #[pyo3(get)]
    record_size: u16,
}

#[pymethods]
impl Metadata {
    /// A dictionary from locale codes to the database description in that locale.
    #[getter]
    fn description(&self, py: Python) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for (key, value) in &self.description_dict {
            dict.set_item(key, value)?;
        }
        Ok(dict.into())
    }

    /// A list of locale codes supported by the database for descriptions and other text.
    #[getter]
    fn languages(&self, py: Python) -> PyResult<Py<PyAny>> {
        let list = PyList::new(py, &self.languages_list)?;
        Ok(list.into_any().unbind())
    }

    /// The size of a node in bytes.
    #[getter]
    fn node_byte_size(&self) -> u16 {
        self.record_size / 4
    }

    /// The size of the search tree in bytes.
    #[getter]
    fn search_tree_size(&self) -> u32 {
        self.node_count * (self.record_size as u32 / 4)
    }
}

impl Metadata {
    #[inline]
    pub(crate) fn from_maxmind(meta: &maxminddb_crate::Metadata) -> Self {
        Self {
            binary_format_major_version: meta.binary_format_major_version,
            binary_format_minor_version: meta.binary_format_minor_version,
            build_epoch: meta.build_epoch,
            database_type: meta.database_type.clone(),
            description_dict: meta.description.clone(),
            ip_version: meta.ip_version,
            languages_list: meta.languages.clone(),
            node_count: meta.node_count,
            record_size: meta.record_size,
        }
    }
}
