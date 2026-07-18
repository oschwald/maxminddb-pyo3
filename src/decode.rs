use pyo3::{
    conversion::IntoPyObjectExt,
    prelude::*,
    types::{PyBytes, PyDict, PyList, PyString},
};
use rustc_hash::FxHashMap;
use serde::de::{self, Deserialize, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use std::{cell::RefCell, fmt};

thread_local! {
    static PY_MAP_KEY_CACHE: RefCell<FxHashMap<String, Py<PyString>>> =
        RefCell::new(FxHashMap::default());
}

const PY_MAP_KEY_CACHE_MAX: usize = 256;

/// Wrapper that owns the Python object produced by deserializing a MaxMind record.
#[derive(Debug)]
pub(crate) struct PyDecodedValue {
    value: Py<PyAny>,
}

impl PyDecodedValue {
    #[inline]
    fn new(value: Py<PyAny>) -> Self {
        Self { value }
    }

    #[inline]
    pub(crate) fn into_py(self) -> Py<PyAny> {
        self.value
    }
}

impl<'de> Deserialize<'de> for PyDecodedValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Python::attach(|py| PyValueSeed { py }.deserialize(deserializer))
    }
}

#[derive(Copy, Clone)]
struct PyValueSeed<'py> {
    py: Python<'py>,
}

impl<'py> PyValueSeed<'py> {
    #[inline]
    fn new(py: Python<'py>) -> Self {
        Self { py }
    }
}

impl<'de, 'py> DeserializeSeed<'de> for PyValueSeed<'py> {
    type Value = PyDecodedValue;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(PyValueVisitor { py: self.py })
    }
}

struct PyValueVisitor<'py> {
    py: Python<'py>,
}

impl<'de, 'py> Visitor<'de> for PyValueVisitor<'py> {
    type Value = PyDecodedValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any valid MaxMind DB value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        bound_to_value(value.into_py_any(self.py))
    }

    fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        bound_to_value(value.into_py_any(self.py))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
            bound_to_value((value as i32).into_py_any(self.py))
        } else {
            Err(E::custom(format!("integer {} out of i32 range", value)))
        }
    }

    fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        bound_to_value(value.into_py_any(self.py))
    }

    fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        bound_to_value(value.into_py_any(self.py))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        bound_to_value(value.into_py_any(self.py))
    }

    fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        bound_to_value(value.into_py_any(self.py))
    }

    fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        bound_to_value(value.into_py_any(self.py))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        bound_to_value(value.into_py_any(self.py))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        bound_to_value(value.into_py_any(self.py))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        bound_to_value(value.into_py_any(self.py))
    }

    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let py_bytes = PyBytes::new(self.py, value);
        Ok(PyDecodedValue::new(py_bytes.into_any().unbind()))
    }

    fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let py_bytes = PyBytes::new(self.py, &value);
        Ok(PyDecodedValue::new(py_bytes.into_any().unbind()))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let capacity = seq.size_hint().unwrap_or(0);
        let mut elements = Vec::with_capacity(capacity);
        while let Some(elem) = seq.next_element_seed(PyValueSeed::new(self.py))? {
            elements.push(elem.into_py());
        }
        let py_list = PyList::new(self.py, elements).map_err(pyerr_to_de_error)?;
        Ok(PyDecodedValue::new(py_list.into_any().unbind()))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let dict = PyDict::new(self.py);
        while let Some(key) = map.next_key::<&'de str>()? {
            let value = map.next_value_seed(PyValueSeed::new(self.py))?;
            set_cached_map_item(self.py, &dict, key, value.into_py()).map_err(pyerr_to_de_error)?;
        }
        Ok(PyDecodedValue::new(dict.into_any().unbind()))
    }
}

fn pyerr_to_de_error<E: de::Error>(err: PyErr) -> E {
    E::custom(err.to_string())
}

fn bound_to_value<E: de::Error>(result: PyResult<Py<PyAny>>) -> Result<PyDecodedValue, E> {
    result.map(PyDecodedValue::new).map_err(pyerr_to_de_error)
}

fn set_cached_map_item(
    py: Python,
    dict: &Bound<'_, PyDict>,
    key: &str,
    value: Py<PyAny>,
) -> PyResult<()> {
    PY_MAP_KEY_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(existing) = cache.get(key) {
            return dict.set_item(existing.bind(py), value);
        }
        if cache.len() >= PY_MAP_KEY_CACHE_MAX {
            return dict.set_item(PyString::new(py, key), value);
        }
        let py_key = PyString::new(py, key).unbind();
        let result = dict.set_item(py_key.bind(py), value);
        cache.insert(key.to_owned(), py_key);
        result
    })
}
