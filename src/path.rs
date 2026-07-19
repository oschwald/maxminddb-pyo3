use crate::maxminddb_crate::PathElement;
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    types::{PyBool, PyInt, PyString, PyTuple},
};

pub(crate) enum OwnedPathElement {
    Key(String),
    Index(usize),
    IndexFromEnd(usize),
}

pub(crate) fn path_tuple_matches_owned(
    path: &Bound<'_, PyTuple>,
    cached: &[OwnedPathElement],
) -> bool {
    path.len() == cached.len()
        && path
            .iter()
            .zip(cached)
            .all(|(item, cached_element)| match cached_element {
                OwnedPathElement::Key(cached_key) => item
                    .cast::<PyString>()
                    .ok()
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| value == cached_key),
                OwnedPathElement::Index(cached_index) => {
                    !item.is_instance_of::<PyBool>()
                        && item
                            .cast::<PyInt>()
                            .ok()
                            .and_then(|value| value.extract::<usize>().ok())
                            .is_some_and(|index| index == *cached_index)
                }
                OwnedPathElement::IndexFromEnd(cached_index) => {
                    !item.is_instance_of::<PyBool>()
                        && item
                            .cast::<PyInt>()
                            .ok()
                            .and_then(|value| value.extract::<isize>().ok())
                            .map(signed_index_to_owned_path_element)
                            .is_some_and(|element| match element {
                                OwnedPathElement::IndexFromEnd(index) => index == *cached_index,
                                OwnedPathElement::Key(_) | OwnedPathElement::Index(_) => false,
                            })
                }
            })
}

pub(crate) fn parse_path(path: &Bound<'_, PyAny>) -> PyResult<Vec<OwnedPathElement>> {
    const ERR_PATH_SEQUENCE: &str = "Path must be a sequence (list or tuple)";
    const ERR_PATH_ELEMENT: &str = "Path elements must be strings or integers";

    if path.is_instance_of::<PyString>() {
        return Err(PyTypeError::new_err(ERR_PATH_SEQUENCE));
    }

    let iterator = path
        .try_iter()
        .map_err(|_| PyTypeError::new_err(ERR_PATH_SEQUENCE))?;
    let mut owned_path = Vec::new();
    for item in iterator {
        let item = item?;
        if item.is_instance_of::<PyBool>() {
            return Err(PyTypeError::new_err(ERR_PATH_ELEMENT));
        }
        if let Ok(s) = item.extract::<String>() {
            owned_path.push(OwnedPathElement::Key(s));
            continue;
        }
        if item.cast::<PyInt>().is_ok() {
            if let Ok(i) = item.extract::<isize>() {
                owned_path.push(signed_index_to_owned_path_element(i));
                continue;
            }
            if let Ok(i) = item.extract::<usize>() {
                owned_path.push(OwnedPathElement::Index(i));
                continue;
            }
        }
        return Err(PyTypeError::new_err(ERR_PATH_ELEMENT));
    }

    Ok(owned_path)
}

#[inline]
fn signed_index_to_owned_path_element(n: isize) -> OwnedPathElement {
    if n >= 0 {
        OwnedPathElement::Index(n as usize)
    } else {
        let index = n
            .checked_neg()
            .and_then(|n| n.checked_sub(1))
            .map(|n| n as usize)
            .unwrap_or(usize::MAX);
        OwnedPathElement::IndexFromEnd(index)
    }
}

pub(crate) fn path_elements_from_owned_path(path: &[OwnedPathElement]) -> Vec<PathElement<'_>> {
    path.iter()
        .map(|element| match element {
            OwnedPathElement::Key(key) => PathElement::Key(key.as_str()),
            OwnedPathElement::Index(index) => PathElement::Index(*index),
            OwnedPathElement::IndexFromEnd(index) => PathElement::IndexFromEnd(*index),
        })
        .collect()
}
