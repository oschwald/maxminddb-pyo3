use maxminddb::Reader as MaxMindReader;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde_json::Value;
use std::net::IpAddr;
use std::path::Path;

/// A Python wrapper around the MaxMind DB reader.
#[pyclass]
struct Reader {
    reader: MaxMindReader<Vec<u8>>,
}

#[pymethods]
impl Reader {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        open_database(path)
    }

    fn get(&self, py: Python, ip: &str) -> PyResult<PyObject> {
        let ip_addr: IpAddr = ip
            .parse()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid IP address"))?;

        // Deserialize into serde_json::Value
        match self.reader.lookup::<Value>(ip_addr) {
            Ok(data) => Ok(self.convert_to_py(py, &data)),
            Err(maxminddb::MaxMindDBError::AddressNotFoundError(_)) => Ok(py.None()),
            Err(_) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Lookup error",
            )),
        }
    }
}

impl Reader {
    /// Recursively convert `serde_json::Value` into Python objects
    fn convert_to_py(&self, py: Python, value: &Value) -> PyObject {
        match value {
            Value::Null => py.None(),
            Value::Bool(b) => b.into_py(py),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    i.into_py(py)
                } else if let Some(f) = n.as_f64() {
                    f.into_py(py)
                } else {
                    py.None()
                }
            }
            Value::String(s) => s.into_py(py),
            Value::Array(arr) => {
                let py_list = PyList::empty(py);
                for item in arr {
                    py_list.append(self.convert_to_py(py, item)).unwrap();
                }
                py_list.into()
            }
            Value::Object(obj) => {
                let py_dict = PyDict::new(py);
                for (key, val) in obj {
                    py_dict.set_item(key, self.convert_to_py(py, val)).unwrap();
                }
                py_dict.into()
            }
        }
    }
}

/// Open the MaxMind database and return a Reader instance
#[pyfunction]
fn open_database(path: &str) -> PyResult<Reader> {
    let reader = MaxMindReader::open_readfile(Path::new(path)).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to open database: {}", e))
    })?;
    Ok(Reader { reader })
}

/// Python module definition
#[pymodule]
fn maxminddb_pyo3(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Reader>()?;
    m.add_function(wrap_pyfunction!(open_database, m)?)?;
    Ok(())
}
