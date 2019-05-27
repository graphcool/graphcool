use crate::dml;
use crate::dml::validator::directive::{Args, DirectiveValidator, Error};

/// Prismas builtin `@primary` directive.
pub struct PrimaryDirectiveValidator {}

impl DirectiveValidator<dml::Field> for PrimaryDirectiveValidator {
    fn directive_name(&self) -> &'static str {
        &"primary"
    }
    fn validate_and_apply(&self, args: &Args, obj: &mut dml::Field) -> Result<(), Error> {
        let mut id_info = dml::IdInfo {
            strategy: dml::IdStrategy::Auto,
            sequence: None,
        };

        if let Ok(arg) = args.arg("name") {
            match arg.parse_literal::<dml::IdStrategy>() {
                Ok(strategy) => id_info.strategy = strategy,
                Err(err) => return self.parser_error(&err),
            }
        }

        obj.id_info = Some(id_info);

        return Ok(());
    }
}