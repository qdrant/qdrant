use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use validator::Validate;

use super::claims::Claims;
use super::AuthError;

#[derive(Clone)]
pub struct JwtParser {
    key: DecodingKey,
    validation: Validation,
}

impl JwtParser {
    const ALGORITHM: Algorithm = Algorithm::HS256;

    pub fn new(secret: &str) -> Self {
        let key = DecodingKey::from_secret(secret.as_bytes());
        let mut validation = Validation::new(Self::ALGORITHM);

        // Qdrant server is the only audience
        validation.validate_aud = false;

        // Expiration time leeway to account for clock skew
        validation.leeway = 30;

        // All claims are optional
        validation.required_spec_claims = Default::default();

        JwtParser { key, validation }
    }

    /// Decode the token and return the claims, this already validates the `exp` claim with some leeway.
    /// Returns None when the token doesn't look like a JWT.
    pub fn decode(&self, token: &str) -> Option<Result<Claims, AuthError>> {
        let claims = match decode::<Claims>(token, &self.key, &self.validation) {
            Ok(token_data) => token_data.claims,
            Err(e) => match e.kind() {
                ErrorKind::ExpiredSignature | ErrorKind::InvalidSignature => {
                    return Some(Err(AuthError::Forbidden(e.to_string())))
                }
                _ => return None,
            },
        };
        if let Err(e) = claims.validate() {
            return Some(Err(AuthError::Unauthorized(e.to_string())));
        }
        Some(Ok(claims))
    }
}

#[cfg(test)]
mod tests {
    use segment::types::ValueVariants;
    use storage::rbac::{
        Access, CollectionAccess, CollectionAccessList, CollectionAccessMode, GlobalAccessMode,
        PayloadConstraint,
    };

    use super::*;

    pub fn create_token(claims: &Claims) -> String {
        use jsonwebtoken::{encode, EncodingKey, Header};

        let key = EncodingKey::from_secret("secret".as_ref());
        let header = Header::new(JwtParser::ALGORITHM);
        encode(&header, claims, &key).unwrap()
    }

    #[test]
    fn test_jwt_parser() {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let claims = Claims {
            exp: Some(exp),
            access: Access::Collection(CollectionAccessList(vec![CollectionAccess {
                collections: vec!["collection".to_string()],
                access: CollectionAccessMode::ReadWrite,
                payload: Some(PayloadConstraint(
                    vec![
                        (
                            "field1".parse().unwrap(),
                            ValueVariants::Keyword("value".to_string()),
                        ),
                        ("field2".parse().unwrap(), ValueVariants::Integer(42)),
                        ("field3".parse().unwrap(), ValueVariants::Bool(true)),
                    ]
                    .into_iter()
                    .collect(),
                )),
            }])),
            value_exists: None,
        };
        let token = create_token(&claims);

        let secret = "secret";
        let parser = JwtParser::new(secret);
        let decoded_claims = parser.decode(&token).unwrap().unwrap();

        assert_eq!(claims, decoded_claims);
    }

    #[test]
    fn test_exp_validation() {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs()
            - 31; // 31 seconds in the past, bigger than the 30 seconds leeway

        let mut claims = Claims {
            exp: Some(exp),
            access: Access::Global(GlobalAccessMode::Read),
            value_exists: None,
        };

        let token = create_token(&claims);

        let secret = "secret";
        let parser = JwtParser::new(secret);
        assert!(matches!(
            parser.decode(&token),
            Some(Err(AuthError::Forbidden(_)))
        ));

        // Remove the exp claim and it should work
        claims.exp = None;
        let token = create_token(&claims);

        let decoded_claims = parser.decode(&token).unwrap().unwrap();

        assert_eq!(claims, decoded_claims);
    }

    #[test]
    fn test_invalid_token() {
        let claims = Claims {
            exp: None,
            access: Access::Global(GlobalAccessMode::Read),
            value_exists: None,
        };
        let token = create_token(&claims);

        assert!(matches!(
            JwtParser::new("wrong-secret").decode(&token),
            Some(Err(AuthError::Forbidden(_)))
        ));

        assert!(JwtParser::new("secret").decode("foo.bar.baz").is_none());
    }
}
