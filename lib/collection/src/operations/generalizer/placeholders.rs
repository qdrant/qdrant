/// This module contains functions to create placeholder JSON values for various data structures.
/// Those placeholders are used in generalizing requests to a more abstract representation,
/// stripping away specific details while retaining the overall structure.
use serde_json::json;

fn closest_power_of_two(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let log2 = (n as f64).log2().round() as u32;
    2_usize.pow(log2)
}

fn closest_power_of_two_float(n: f64) -> f64 {
    let n_abs = n.abs();
    if n_abs < f64::EPSILON {
        return 0.0;
    }
    let log2 = n.log2().round() as u32;
    let abs_res = 2_f64.powi(log2 as i32);
    if n.is_sign_negative() {
        -abs_res
    } else {
        abs_res
    }
}

pub(crate) fn float_value_placeholder(float: f64) -> serde_json::Value {
    json!({
        "type": "float",
        "approx_value": closest_power_of_two_float(float.abs())
    })
}

pub(crate) fn size_value_placeholder(int: usize) -> serde_json::Value {
    json!({
        "type": "usize",
        "approx_value": closest_power_of_two(int)
    })
}

pub(crate) fn values_array_placeholder(len: usize, approx_len: bool) -> serde_json::Value {
    if approx_len {
        let approx_len = closest_power_of_two(len);
        return json!({
            "type": "values_array",
            "approx_len": approx_len
        });
    }

    json!({
        "type": "values_array",
        "len": len
    })
}

pub(crate) fn text_placeholder(text: &str, approx_len: bool) -> serde_json::Value {
    let char_len = text.len(); // Assuming 1 byte per char for simplicity
    let word_len = text.split_whitespace().count();

    if approx_len {
        return json!({
            "type": "text",
            "approx_chars": closest_power_of_two(char_len),
            "approx_words": closest_power_of_two(word_len)
        });
    }

    json!({
        "type": "text",
        "chars": char_len,
        "words": word_len
    })
}
