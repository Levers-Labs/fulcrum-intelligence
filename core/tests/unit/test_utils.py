from fulcrum_core.utilities.camel_to_snake_case import convert_keys_camel_to_snake_case


def test_convert_keys_camel_to_snake_case():
    first_level = {"SomeKey": "SomeValue", "AnotherKey": "AnotherValue"}
    second_level = {"SomeKey": {"AnotherKey": "AnotherValue", "TestKey": "TestValue"}}
    third_level = {"SomeKey": {"AnotherKey": {"TestKey": "TestValue"}}}
    all_levels_with_lists = {
        "SomeKey": [{"AnotherKey": "AnotherValue"}, {"TestKey": [{"AnotherKey": "AnotherValue"}, "TestValue"]}],
        "StringVal": "StringVal",
    }

    assert convert_keys_camel_to_snake_case(dict(), first_level) == {
        "some_key": "SomeValue",
        "another_key": "AnotherValue",
    }
    assert convert_keys_camel_to_snake_case(dict(), second_level) == {
        "some_key": {"another_key": "AnotherValue", "test_key": "TestValue"}
    }
    assert convert_keys_camel_to_snake_case(dict(), third_level) == {
        "some_key": {"another_key": {"test_key": "TestValue"}}
    }
    assert convert_keys_camel_to_snake_case(dict(), all_levels_with_lists) == {
        "some_key": [
            {"another_key": "AnotherValue"},
            {"test_key": [{"another_key": "AnotherValue"}, "TestValue"]},
        ],
        "string_val": "StringVal",
    }
