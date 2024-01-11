use common::types::PointOffsetType;
use tempfile::Builder;

use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
use crate::data_types::text_index::{TextIndexParams, TokenizerType};
use crate::index::field_index::full_text_index::text_index::FullTextIndex;
use crate::index::field_index::ValueIndexer;

fn get_texts() -> Vec<String> {
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
        tokenizer: TokenizerType::Prefix,
        min_token_len: None,
        max_token_len: None,
        lowercase: None,
    };

    let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();
    let mut index = FullTextIndex::new(db, config, "text");
    index.recreate().unwrap();

    let texts = get_texts();

    for (i, text) in texts.iter().enumerate() {
        index
            .add_many(i as PointOffsetType, vec![text.to_string()])
            .unwrap();
    }

    let res: Vec<_> = index.query("ROBO").collect();

    let query = index.parse_query("ROBO");

    for idx in res.iter() {
        let doc = index.get_doc(*idx).unwrap();
        assert!(query.check_match(doc));
    }

    assert_eq!(res.len(), 3);

    let res: Vec<_> = index.query("q231").collect();

    let query = index.parse_query("q231");

    for idx in [1, 2, 3] {
        let doc = index.get_doc(idx).unwrap();
        assert!(!query.check_match(doc));
    }

    assert_eq!(res.len(), 0);
}
