use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::PyString,
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Parse an IP address from a string or an `ipaddress` object.
#[inline(always)]
pub(crate) fn parse_ip_address(ip_address: &Bound<'_, PyAny>) -> PyResult<IpAddr> {
    // Fast path: Try string first (most common case)
    if let Ok(py_str) = ip_address.cast::<PyString>() {
        return parse_ip_string(py_str.to_str()?);
    }

    // Slow path: Check if it's an ipaddress.IPv4Address or IPv6Address
    let type_name = ip_address.get_type().name()?;
    if type_name == "IPv4Address" {
        // The stdlib classes store their numeric address in `_ip`. Reading it
        // avoids allocating the temporary `packed` bytes used by PyO3's generic
        // IpAddr conversion. Fall back in case another implementation differs.
        if let Ok(value) = ip_address
            .getattr(pyo3::intern!(ip_address.py(), "_ip"))
            .and_then(|value| value.extract::<u32>())
        {
            return Ok(IpAddr::V4(Ipv4Addr::from(value)));
        }
        return ip_address.extract::<IpAddr>();
    }
    if type_name == "IPv6Address" {
        if let Ok(value) = ip_address
            .getattr(pyo3::intern!(ip_address.py(), "_ip"))
            .and_then(|value| value.extract::<u128>())
        {
            return Ok(IpAddr::V6(Ipv6Addr::from(value)));
        }
        return ip_address.extract::<IpAddr>();
    }

    Err(PyTypeError::new_err(
        "argument 1 must be a string or ipaddress object",
    ))
}

#[inline(always)]
fn parse_ip_string(s: &str) -> PyResult<IpAddr> {
    if let Some(ip) = parse_ipv4_string(s.as_bytes()) {
        return Ok(IpAddr::V4(ip));
    }

    s.parse().map_err(|_| {
        PyValueError::new_err(format!(
            "'{}' does not appear to be an IPv4 or IPv6 address",
            s
        ))
    })
}

#[inline(always)]
fn parse_ipv4_string(bytes: &[u8]) -> Option<Ipv4Addr> {
    let mut octets = [0u8; 4];
    let mut octet_index = 0;
    let mut value: u16 = 0;
    let mut digits = 0;

    for &byte in bytes {
        if byte == b'.' {
            if digits == 0 || octet_index == 3 {
                return None;
            }
            octets[octet_index] = value as u8;
            octet_index += 1;
            value = 0;
            digits = 0;
            continue;
        }

        if !byte.is_ascii_digit() {
            return None;
        }
        if digits == 1 && value == 0 {
            return None;
        }

        digits += 1;
        if digits > 3 {
            return None;
        }
        value = value * 10 + u16::from(byte - b'0');
        if value > u16::from(u8::MAX) {
            return None;
        }
    }

    if octet_index != 3 || digits == 0 {
        return None;
    }
    octets[octet_index] = value as u8;

    Some(Ipv4Addr::from(octets))
}

/// Generate the error message for an IPv6 lookup in an IPv4 database.
#[inline]
pub(crate) fn ipv6_in_ipv4_error(ip: &IpAddr) -> String {
    format!(
        "Error looking up {}. You attempted to look up an IPv6 address in an IPv4-only database",
        ip
    )
}

#[cfg(test)]
mod tests {
    use super::parse_ipv4_string;
    use std::net::Ipv4Addr;

    #[test]
    fn parses_strict_ipv4_strings() {
        assert_eq!(
            parse_ipv4_string(b"0.1.2.255"),
            Some(Ipv4Addr::new(0, 1, 2, 255))
        );
        assert_eq!(
            parse_ipv4_string(b"192.0.2.1"),
            Some(Ipv4Addr::new(192, 0, 2, 1))
        );
    }

    #[test]
    fn rejects_ipv4_strings_that_std_parser_rejects() {
        for value in [
            b"01.2.3.4".as_slice(),
            b"1.02.3.4".as_slice(),
            b"1.2.3.04".as_slice(),
            b"1.2.3".as_slice(),
            b"1.2.3.4.5".as_slice(),
            b"1..2.3".as_slice(),
            b"256.1.1.1".as_slice(),
            b"1.2.3.4 ".as_slice(),
            b" 1.2.3.4".as_slice(),
            b"2001:db8::1".as_slice(),
        ] {
            assert_eq!(parse_ipv4_string(value), None);
        }
    }
}
