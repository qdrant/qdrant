import random

import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection_fts'

texts = [
    "2430 A.D.",
    "The Acquisitive Chuckle",
    "Author! Author!",
    "The Bicentennial Man",
    "Big Game",
    "The Billiard Ball",
    "Birth of a Notion",
    "Black Friar of the Flame",
    "Blank!",
    "Blind Alley",
    "Breeds There a Man...?",
    "Button, Button",
    "Buy Jupiter",
    "C-Chute",
    "Cal",
    "The Callistan Menace",
    "Catch That Rabbit",
    "Christmas on Ganymede",
    "Darwinian Pool Room",
    "Day of the Hunters",
    "Death Sentence",
    "Does a Bee Care?",
    "Dreaming Is a Private Thing",
    "The Dust of Death",
    "The Dying Night",
    "Each an Explorer",
    "Escape!",
    "Everest",
    "Evidence",
    "The Evitable Conflict",
    "Exile to Hell",
    "Eyes Do More Than See",
    "The Feeling of Power",
    "Feminine Intuition",
    "First Law",
    "Flies",
    "For the Birds",
    "Founding Father",
    "The Fun They Had",
    "Galley Slave",
    "The Gentle Vultures",
    "Getting Even",
    "Gimmicks Three",
    "Gold",
    "Good Taste",
    "The Greatest Asset",
    "Green Patches",
    "Half-Breed",
    "Half-Breeds on Venus",
    "Hallucination",
    "The Hazing",
    "Hell-Fire",
    "Heredity",
    "History",
    "Homo Sol",
    "Hostess",
    "I Just Make Them Up, See!",
    "I'm in Marsport Without Hilda",
    "The Imaginary",
    "The Immortal Bard",
    "In a Good Cause—",
    "Insert Knob A in Hole B",
    "The Instability",
    "It's Such a Beautiful Day",
    "The Key",
    "Kid Stuff",
    "The Last Answer",
    "The Last Question",
    "The Last Trump",
    "Left to Right",
    "Legal Rites",
    "Lenny",
    "Lest We Remember",
    "Let's Not",
    "Liar!",
    "Light Verse",
    "Little Lost Robot",
    "The Little Man on the Subway",
    "Living Space",
    "A Loint of Paw",
    "The Magnificent Possession",
    "Marching In",
    "Marooned off Vesta",
    "The Message",
    "Mirror Image",
    "Mother Earth",
    "My Son, the Physicist",
    "No Connection",
    "No Refuge Could Save",
    "Nobody Here But—",
    "Not Final!",
    "Obituary",
    "Old-fashioned",
    "Pâté de Foie Gras",
    "The Pause",
    "Ph as in Phony",
    "The Portable Star",
    "The Proper Study",
    "Rain, Rain, Go Away",
    "Reason",
    "The Red Queen's Race",
    "Rejection Slips",
    "Ring Around the Sun",
    "Risk",
    "Robot AL-76 Goes Astray",
    "Robot Dreams",
    "Runaround",
    "Sally",
    "Satisfaction Guaranteed",
    "The Secret Sense",
    "Shah Guido G.",
    "Silly Asses",
    "The Singing Bell",
    "Sixty Million Trillion Combinations",
    "Spell My Name with an S",
    "Star Light",
    "A Statue for Father",
    "Strikebreaker",
    "Super-Neutron",
    "Take a Match",
    "The Talking Stone",
    ". . . That Thou Art Mindful of Him",
    "Thiotimoline",
    "Time Pussy",
    "Trends",
    "Truth to Tell",
    "The Ugly Little Boy",
    "The Ultimate Crime",
    "Unto the Fourth Generation",
    "The Up-to-Date Sorcerer",
    "Waterclap",
    "The Watery Place",
    "The Weapon",
    "The Weapon Too Dreadful to Use",
    "What If—",
    "What Is This Thing Called Love?",
    "What's in a Name?",
    "The Winnowing",
]


def basic_collection_setup(
    collection_name='test_collection',
    on_disk_vectors=False,
    on_disk_payload=False,
):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 4,
                "distance": "Dot",
                "on_disk": on_disk_vectors,
            },
            "on_disk_payload": on_disk_payload,
        }
    )
    assert response.ok

    # Create index
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": "title",
            "field_schema": {
                "type": "text",
                "tokenizer": "prefix",
            }
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": idx,
                    "vector": [random.random() for _ in range(4)],
                    "payload": {"title": title}
                } for idx, title in enumerate(texts)
            ]
        }
    )
    assert response.ok


@pytest.fixture(autouse=True, scope='module')
def setup(on_disk_vectors, on_disk_payload):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors, on_disk_payload=on_disk_payload)
    yield
    drop_collection(collection_name=collection_name)


def test_scroll_with_prefix():
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "offset": None,
            "limit": 10,
            "with_payload": True,
            "with_vector": False,
            "filter": {
                "should": [
                    {
                        "key": "title",
                        "match": {
                            "text": "ROBO",
                        }
                    }
                ]
            }
        }
    )

    assert response.ok
    assert len(response.json()['result']['points']) == 3

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [1., 1., 1., 1.],
            "limit": 10,
            "with_payload": True,
            "with_vector": False,
            "filter": {
                "should": [
                    {
                        "key": "title",
                        "match": {
                            "text": "ROBO",
                        }
                    }
                ]
            }
        }
    )

    assert response.ok
    assert len(response.json()['result']) == 3
