use jsonwebtoken::errors::Error;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};

use super::claims::Claims;

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

    /// Decode the token and return the claims, this already validates the `exp` claim with some leeway
    pub fn decode(&self, token: &str) -> Result<Claims, Error> {
        let claims = decode::<Claims>(token, &self.key, &self.validation)?.claims;

        Ok(claims)
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
        let decoded_claims = parser.decode(&token).unwrap();

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
        assert!(parser.decode(&token).is_err());

        // Remove the exp claim and it should work
        claims.exp = None;
        let token = create_token(&claims);

        let decoded_claims = parser.decode(&token).unwrap();

        assert_eq!(claims, decoded_claims);
    }
}
