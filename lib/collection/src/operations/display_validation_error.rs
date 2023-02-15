use validator::{ValidationErrors, ValidationErrorsKind};

fn display_recursive(pre_path: &str, errors: &ValidationErrors) -> Vec<(String, String)> {
    let mut path = pre_path.to_string();

    let mut res = Vec::new();
    for (curr_path, rest) in errors.errors() {
        path = format!("{}.{}", path, curr_path);
        let errs = match rest {
            ValidationErrorsKind::Struct(err) => display_recursive(&path, err),
            ValidationErrorsKind::List(errors) => errors
                .iter()
                .map(|(i, err)| display_recursive(&format!("{}[{}]", path, i), err))
                .flatten()
                .collect(),
            ValidationErrorsKind::Field(errors) => errors
                .iter()
                .map(|err| (path.clone(), format!("{}", err)))
                .collect(),
        };
        res.extend(errs);
    }
    res
}


#[cfg(test)]
mod tests {
    use validator::Validate;
    use super::*;

    #[derive(Validate, Debug)]
    struct SomeThing {
        #[validate(range(min = 1))]
        pub idx: usize,
    }

    #[derive(Validate, Debug)]
    struct OtherThing {
        pub things: Vec<SomeThing>,
    }

    #[test]
    fn test_validation_render() {
        let bad_config = OtherThing { things: vec![
            SomeThing { idx: 0 },
            SomeThing { idx: 1 },
            SomeThing { idx: 2 },
        ] };

        let errors = bad_config.things[0].validate().unwrap_err();

        eprintln!("bad_config = {:#?}", bad_config);

        eprintln!("errors = {}", errors);

        let errs = display_recursive("", &errors);
        for (path, msg) in errs {
            println!("{}: {}", path, msg);
        }
    }
}