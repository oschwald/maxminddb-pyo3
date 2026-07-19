use crate::{
    decode::PyDecodedValue,
    maxminddb_crate::{self, MaxMindDbError, Within, WithinOptions},
    InvalidDatabaseError, ReaderSource,
};
use memmap2::Mmap;
use pyo3::prelude::*;
use self_cell::self_cell;
use std::{str::FromStr, sync::Arc};

/// Iterator for Reader that yields `(network, record)` tuples.
#[pyclass(module = "maxminddb_rust")]
pub(crate) struct ReaderIterator {
    iter: ReaderWithin,
    ipv4_network_cls: Py<PyAny>,
    ipv6_network_cls: Py<PyAny>,
}

impl ReaderIterator {
    #[inline]
    fn root_network_for_ip_version(
        ip_version: u16,
    ) -> PyResult<(ipnetwork::IpNetwork, &'static str)> {
        let (network_str, network_type) = if ip_version == 4 {
            ("0.0.0.0/0", "IPv4")
        } else {
            ("::/0", "IPv6")
        };

        let network = ipnetwork::IpNetwork::from_str(network_str).map_err(|err| {
            InvalidDatabaseError::new_err(format!(
                "Failed to create {} network: {}",
                network_type, err
            ))
        })?;
        Ok((network, network_type))
    }

    #[inline]
    fn network_to_python(&self, py: Python, ip_net: ipnetwork::IpNetwork) -> PyResult<Py<PyAny>> {
        let (class, addr_int, prefix_len) = match ip_net {
            ipnetwork::IpNetwork::V4(v4) => (
                self.ipv4_network_cls.bind(py),
                u32::from(v4.ip()) as u128,
                v4.prefix(),
            ),
            ipnetwork::IpNetwork::V6(v6) => (
                self.ipv6_network_cls.bind(py),
                u128::from(v6.ip()),
                v6.prefix(),
            ),
        };
        Ok(class.call1(((addr_int, prefix_len),))?.unbind())
    }

    pub(crate) fn new(py: Python, reader: Arc<ReaderSource>) -> PyResult<Self> {
        let ip_version = reader.metadata().ip_version;
        let (network, network_type) = Self::root_network_for_ip_version(ip_version)?;

        let iter = ReaderWithin::new(reader, network).map_err(|err| {
            InvalidDatabaseError::new_err(format!("Failed to iterate {}: {}", network_type, err))
        })?;

        let ipaddress = py.import("ipaddress")?;
        let ipv4_network_cls = ipaddress.getattr("IPv4Network")?.unbind();
        let ipv6_network_cls = ipaddress.getattr("IPv6Network")?.unbind();

        Ok(Self {
            iter,
            ipv4_network_cls,
            ipv6_network_cls,
        })
    }
}

#[pymethods]
impl ReaderIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<(Py<PyAny>, Py<PyAny>)>> {
        let next_item = match self.iter.next() {
            Some(result) => result
                .map_err(|err| InvalidDatabaseError::new_err(format!("Iteration error: {err}")))?,
            None => return Ok(None),
        };
        let network_obj = self.network_to_python(py, next_item.ip_net)?;
        let data_obj = next_item.data.into_py();

        Ok(Some((network_obj, data_obj)))
    }
}

type MmapWithin<'a> = Within<'a, Mmap>;
type MemoryWithin<'a> = Within<'a, Vec<u8>>;

self_cell!(
    struct MmapWithinCell {
        owner: Arc<ReaderSource>,

        #[covariant]
        dependent: MmapWithin,
    }
);

self_cell!(
    struct MemoryWithinCell {
        owner: Arc<ReaderSource>,

        #[covariant]
        dependent: MemoryWithin,
    }
);

enum ReaderWithin {
    Mmap(MmapWithinCell),
    Memory(MemoryWithinCell),
}

/// Result from the within iterator, containing network and decoded data.
struct WithinResult {
    ip_net: ipnetwork::IpNetwork,
    data: PyDecodedValue,
}

#[inline]
fn process_within_lookup<S: AsRef<[u8]>>(
    lookup_result: maxminddb_crate::LookupResult<'_, S>,
) -> Result<WithinResult, MaxMindDbError> {
    let network = lookup_result.network()?;
    let ip_net = ipnetwork::IpNetwork::new(network.network(), network.prefix()).map_err(|err| {
        MaxMindDbError::InvalidDatabase {
            message: format!("Invalid network from database: {err}"),
            offset: None,
        }
    })?;
    let data: Option<PyDecodedValue> = lookup_result.decode()?;
    let data = data.ok_or_else(|| MaxMindDbError::InvalidDatabase {
        message: "No data in database record".to_string(),
        offset: None,
    })?;
    Ok(WithinResult { ip_net, data })
}

#[inline]
fn next_within<S: AsRef<[u8]>>(
    iter: &mut Within<'_, S>,
) -> Option<Result<WithinResult, MaxMindDbError>> {
    let result = iter.next()?;
    Some(result.and_then(process_within_lookup))
}

impl ReaderWithin {
    fn new(
        reader: Arc<ReaderSource>,
        network: ipnetwork::IpNetwork,
    ) -> Result<Self, maxminddb_crate::MaxMindDbError> {
        let options = WithinOptions::default();
        match reader.as_ref() {
            ReaderSource::Mmap(_) => Ok(Self::Mmap(MmapWithinCell::try_new(
                reader,
                move |reader| match reader.as_ref() {
                    ReaderSource::Mmap(inner) => inner.within(network, options),
                    ReaderSource::Memory(_) => {
                        unreachable!("reader source changed while building iterator")
                    }
                },
            )?)),
            ReaderSource::Memory(_) => Ok(Self::Memory(MemoryWithinCell::try_new(
                reader,
                move |reader| match reader.as_ref() {
                    ReaderSource::Mmap(_) => {
                        unreachable!("reader source changed while building iterator")
                    }
                    ReaderSource::Memory(inner) => inner.within(network, options),
                },
            )?)),
        }
    }

    fn next(&mut self) -> Option<Result<WithinResult, maxminddb_crate::MaxMindDbError>> {
        match self {
            ReaderWithin::Mmap(cell) => cell.with_dependent_mut(|_, iter| next_within(iter)),
            ReaderWithin::Memory(cell) => cell.with_dependent_mut(|_, iter| next_within(iter)),
        }
    }
}
