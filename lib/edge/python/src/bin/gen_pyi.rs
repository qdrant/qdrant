fn main() {
    for alias in qdrant_edge::type_hint::ALIASES {
        let hint = unqualify(&alias.definition.to_string());
        println!("{}: TypeAlias = {hint}", alias.name);
    }
}

fn unqualify(hint: &str) -> String {
    let elided = ["builtins.", "collections.abc."];
    hint.split_inclusive(['[', ']', '|', ',', ' '])
        .map(|ty| elided.iter().find_map(|m| ty.strip_prefix(m)).unwrap_or(ty))
        .collect()
}
