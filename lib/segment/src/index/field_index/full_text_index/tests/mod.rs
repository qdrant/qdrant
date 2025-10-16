mod test_congruence;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use tempfile::Builder;

use crate::data_types::index::{TextIndexParams, TextIndexType, TokenizerType};
use crate::index::field_index::full_text_index::text_index::FullTextIndex;
use crate::index::field_index::{FieldIndexBuilderTrait as _, ValueIndexer};

fn movie_titles() -> Vec<String> {
    vec![
        "2430 A.D.".to_string(),
        "The Acquisitive Chuckle".to_string(),
        "Author! Author!".to_string(),
        "The Bicentennial Man".to_string(),
        "Big Game".to_string(),
        "The Billiard Ball".to_string(),
        "Birth of a Notion".to_string(),
        "Black Friar of the Flame".to_string(),
        "Blank!".to_string(),
        "Blind Alley".to_string(),
        "Breeds There a Man...?".to_string(),
        "Button, Button".to_string(),
        "Buy Jupiter".to_string(),
        "C-Chute".to_string(),
        "Cal".to_string(),
        "The Callistan Menace".to_string(),
        "Catch That Rabbit".to_string(),
        "Christmas on Ganymede".to_string(),
        "Darwinian Pool Room".to_string(),
        "Day of the Hunters".to_string(),
        "Death Sentence".to_string(),
        "Does a Bee Care?".to_string(),
        "Dreaming Is a Private Thing".to_string(),
        "The Dust of Death".to_string(),
        "The Dying Night".to_string(),
        "Each an Explorer".to_string(),
        "Escape!".to_string(),
        "Everest".to_string(),
        "Evidence".to_string(),
        "The Evitable Conflict".to_string(),
        "Exile to Hell".to_string(),
        "Eyes Do More Than See".to_string(),
        "The Feeling of Power".to_string(),
        "Feminine Intuition".to_string(),
        "First Law".to_string(),
        "Flies".to_string(),
        "For the Birds".to_string(),
        "Founding Father".to_string(),
        "The Fun They Had".to_string(),
        "Galley Slave".to_string(),
        "The Gentle Vultures".to_string(),
        "Getting Even".to_string(),
        "Gimmicks Three".to_string(),
        "Gold".to_string(),
        "Good Taste".to_string(),
        "The Greatest Asset".to_string(),
        "Green Patches".to_string(),
        "Half-Breed".to_string(),
        "Half-Breeds on Venus".to_string(),
        "Hallucination".to_string(),
        "The Hazing".to_string(),
        "Hell-Fire".to_string(),
        "Heredity".to_string(),
        "History".to_string(),
        "Homo Sol".to_string(),
        "Hostess".to_string(),
        "I Just Make Them Up, See!".to_string(),
        "I'm in Marsport Without Hilda".to_string(),
        "The Imaginary".to_string(),
        "The Immortal Bard".to_string(),
        "In a Good Cause—".to_string(),
        "Insert Knob A in Hole B".to_string(),
        "The Instability".to_string(),
        "It's Such a Beautiful Day".to_string(),
        "The Key".to_string(),
        "Kid Stuff".to_string(),
        "The Last Answer".to_string(),
        "The Last Question".to_string(),
        "The Last Trump".to_string(),
        "Left to Right".to_string(),
        "Legal Rites".to_string(),
        "Lenny".to_string(),
        "Lest We Remember".to_string(),
        "Let's Not".to_string(),
        "Liar!".to_string(),
        "Light Verse".to_string(),
        "Little Lost Robot".to_string(),
        "The Little Man on the Subway".to_string(),
        "Living Space".to_string(),
        "A Loint of Paw".to_string(),
        "The Magnificent Possession".to_string(),
        "Marching In".to_string(),
        "Marooned off Vesta".to_string(),
        "The Message".to_string(),
        "Mirror Image".to_string(),
        "Mother Earth".to_string(),
        "My Son, the Physicist".to_string(),
        "No Connection".to_string(),
        "No Refuge Could Save".to_string(),
        "Nobody Here But—".to_string(),
        "Not Final!".to_string(),
        "Obituary".to_string(),
        "Old-fashioned".to_string(),
        "Pâté de Foie Gras".to_string(),
        "The Pause".to_string(),
        "Ph as in Phony".to_string(),
        "The Portable Star".to_string(),
        "The Proper Study".to_string(),
        "Rain, Rain, Go Away".to_string(),
        "Reason".to_string(),
        "The Red Queen's Race".to_string(),
        "Rejection Slips".to_string(),
        "Ring Around the Sun".to_string(),
        "Risk".to_string(),
        "Robot AL-76 Goes Astray".to_string(),
        "Robot Dreams".to_string(),
        "Runaround".to_string(),
        "Sally".to_string(),
        "Satisfaction Guaranteed".to_string(),
        "The Secret Sense".to_string(),
        "Shah Guido G.".to_string(),
        "Silly Asses".to_string(),
        "The Singing Bell".to_string(),
        "Sixty Million Trillion Combinations".to_string(),
        "Spell My Name with an S".to_string(),
        "Star Light".to_string(),
        "A Statue for Father".to_string(),
        "Strikebreaker".to_string(),
        "Super-Neutron".to_string(),
        "Take a Match".to_string(),
        "The Talking Stone".to_string(),
        ". . . That Thou Art Mindful of Him".to_string(),
        "Thiotimoline".to_string(),
        "Time Pussy".to_string(),
        "Trends".to_string(),
        "Truth to Tell".to_string(),
        "The Ugly Little Boy".to_string(),
        "The Ultimate Crime".to_string(),
        "Unto the Fourth Generation".to_string(),
        "The Up-to-Date Sorcerer".to_string(),
        "Waterclap".to_string(),
        "The Watery Place".to_string(),
        "The Weapon".to_string(),
        "The Weapon Too Dreadful to Use".to_string(),
        "What If—".to_string(),
        "What Is This Thing Called Love?".to_string(),
        "What's in a Name?".to_string(),
        "The Winnowing".to_string(),
    ]
}

#[test]
fn test_prefix_search() {
    let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
    let config = TextIndexParams {
        r#type: TextIndexType::Text,
        tokenizer: TokenizerType::Prefix,
        min_token_len: None,
        max_token_len: None,
        lowercase: None,
        phrase_matching: None,
        stopwords: None,
        on_disk: None,
        stemmer: None,
        ascii_folding: None,
    };

    let mut index =
        FullTextIndex::new_gridstore(temp_dir.path().to_path_buf(), config.clone(), true)
            .unwrap()
            .unwrap();

    let hw_counter = HardwareCounterCell::new();

    let texts = movie_titles();

    for (i, text) in texts.iter().enumerate() {
        index
            .add_many(i as PointOffsetType, vec![text.to_string()], &hw_counter)
            .unwrap();
    }

    let res: Vec<_> = index.query("ROBO", &hw_counter).collect();

    let query = index.parse_text_query("ROBO", &hw_counter).unwrap();

    for idx in res.iter().copied() {
        assert!(index.check_match(&query, idx));
    }

    assert_eq!(res.len(), 3);

    let res: Vec<_> = index.query("q231", &hw_counter).collect();
    assert!(res.is_empty());

    assert!(index.parse_text_query("q231", &hw_counter).is_none());
}

#[test]
fn test_phrase_matching() {
    let hw_counter = HardwareCounterCell::default();

    // Create a text index with phrase matching enabled
    let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
    let config = TextIndexParams {
        r#type: TextIndexType::Text,
        tokenizer: TokenizerType::default(),
        min_token_len: None,
        max_token_len: None,
        lowercase: Some(true),
        on_disk: None,
        phrase_matching: Some(true), // Enable phrase matching
        stopwords: None,
        stemmer: None,
        ascii_folding: None,
    };

    let mut mutable_index =
        FullTextIndex::builder_gridstore(temp_dir.path().to_path_buf(), config.clone())
            .make_empty()
            .unwrap();

    let mut mmap_builder =
        FullTextIndex::builder_mmap(temp_dir.path().to_path_buf(), config.clone(), true);
    mmap_builder.init().unwrap();

    // Add some test documents with phrases
    let documents = vec![
        (0, "the quick brown fox jumps over the lazy dog".to_string()),
        (1, "brown fox quick the jumps over lazy dog".to_string()),
        (2, "quick brown fox runs fast".to_string()),
        (3, "the lazy dog sleeps peacefully".to_string()),
        (4, "the brown brown fox".to_string()),
    ];

    for (point_id, text) in documents {
        mutable_index
            .add_many(point_id, vec![text.clone()], &hw_counter)
            .unwrap();
        mmap_builder
            .add_many(point_id, vec![text], &hw_counter)
            .unwrap();
    }

    let mmap_index = mmap_builder.finalize().unwrap();

    let check_matching = |index: FullTextIndex| {
        // Test regular text matching (should match documents containing all tokens regardless of order)
        let text_query = index
            .parse_text_query("quick brown fox", &hw_counter)
            .unwrap();
        assert!(index.check_match(&text_query, 0));
        assert!(index.check_match(&text_query, 1));
        assert!(index.check_match(&text_query, 2));

        let text_results: Vec<_> = index.filter_query(text_query, &hw_counter).collect();

        // Should match documents 0, 1, and 2 (all contain "quick", "brown", "fox")
        assert_eq!(text_results.len(), 3);
        assert!(text_results.contains(&0));
        assert!(text_results.contains(&1));
        assert!(text_results.contains(&2));

        // Test phrase matching (should only match documents with exact phrase in order)
        let phrase_query = index
            .parse_phrase_query("quick brown fox", &hw_counter)
            .unwrap();
        assert!(index.check_match(&phrase_query, 0));
        assert!(index.check_match(&phrase_query, 2));

        let phrase_results: Vec<_> = index.filter_query(phrase_query, &hw_counter).collect();

        // Should only match documents 0 and 2 (contain "quick brown fox" in that exact order)
        assert_eq!(phrase_results.len(), 2);
        assert!(phrase_results.contains(&0));
        assert!(phrase_results.contains(&2));
        assert!(!phrase_results.contains(&1)); // Document 1 has the words but not in the right order

        // Test phrase that doesn't exist
        let missing_query = index
            .parse_phrase_query("fox brown quick", &hw_counter)
            .unwrap();
        let missing_results: Vec<_> = index.filter_query(missing_query, &hw_counter).collect();

        // Should match no documents (no document contains this exact phrase)
        assert_eq!(missing_results.len(), 0);

        // Test valid phrase up to a token that doesn't exist
        let query_with_unknown_token = index.parse_phrase_query("quick brown bird", &hw_counter);
        // the phrase query is not valid because it contains an unknown token
        assert!(query_with_unknown_token.is_none());

        // Test repeated words
        let phrase_query = index
            .parse_phrase_query("brown brown fox", &hw_counter)
            .unwrap();
        assert!(index.check_match(&phrase_query, 4));

        // Should only match document 4
        let filter_results: Vec<_> = index.filter_query(phrase_query, &hw_counter).collect();
        assert_eq!(filter_results.len(), 1);
        assert!(filter_results.contains(&4));
    };

    check_matching(mutable_index);
    check_matching(mmap_index);
}

#[test]
fn test_ascii_folding_in_full_text_index_word() {
    let hw_counter = HardwareCounterCell::default();

    let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
    let config_enabled = TextIndexParams {
        r#type: TextIndexType::Text,
        tokenizer: TokenizerType::Word,
        min_token_len: None,
        max_token_len: None,
        lowercase: None,
        on_disk: None,
        phrase_matching: None,
        stopwords: None,
        stemmer: None,
        ascii_folding: Some(true),
    };
    let config_disabled = TextIndexParams {
        ascii_folding: Some(false),
        ..config_enabled.clone()
    };

    // Index with folding enabled
    let mut index_enabled =
        FullTextIndex::new_gridstore(temp_dir.path().to_path_buf(), config_enabled.clone(), true)
            .unwrap()
            .unwrap();

    // Index with folding disabled (separate storage path)
    let temp_dir2 = Builder::new().prefix("test_dir").tempdir().unwrap();
    let mut index_disabled = FullTextIndex::new_gridstore(
        temp_dir2.path().to_path_buf(),
        config_disabled.clone(),
        true,
    )
    .unwrap()
    .unwrap();

    // Documents containing accents
    let docs = vec![
        (0, "ação no coração".to_string()),
        (1, "café com leite".to_string()),
    ];

    for (id, text) in &docs {
        index_enabled
            .add_many(*id as PointOffsetType, vec![text.clone()], &hw_counter)
            .unwrap();
        index_disabled
            .add_many(*id as PointOffsetType, vec![text.clone()], &hw_counter)
            .unwrap();
    }

    // ASCII-only queries should match only when folding is enabled
    let query_enabled = index_enabled.parse_text_query("acao", &hw_counter).unwrap();
    assert!(index_enabled.check_match(&query_enabled, 0));

    let results_enabled: Vec<_> = index_enabled
        .filter_query(query_enabled, &hw_counter)
        .collect();
    assert!(results_enabled.contains(&0));

    let query_disabled_opt = index_disabled.parse_text_query("acao", &hw_counter);
    // Query might still parse, but should not match anything
    if let Some(query_disabled) = query_disabled_opt {
        let results_disabled: Vec<_> = index_disabled
            .filter_query(query_disabled, &hw_counter)
            .collect();
        assert!(!results_disabled.contains(&0));
    }

    // Non-folded query must work in both
    let query_acento = index_enabled.parse_text_query("ação", &hw_counter).unwrap();
    assert!(index_enabled.check_match(&query_acento, 0));
    let results_acento: Vec<_> = index_enabled
        .filter_query(query_acento, &hw_counter)
        .collect();
    assert!(results_acento.contains(&0));

    let query_acento2 = index_disabled
        .parse_text_query("ação", &hw_counter)
        .unwrap();
    let results_acento2: Vec<_> = index_disabled
        .filter_query(query_acento2, &hw_counter)
        .collect();
    assert!(results_acento2.contains(&0));
}
