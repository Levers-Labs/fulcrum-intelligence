import re


def convert_keys_camel_to_snake_case(processed_response: dict, insight_response: dict) -> dict:
    """
    In this function we are recursively converting
    - camel case keys to snake case
    - replacing "baseline" with "evaluation" in keys
    Using recursion to perform above 2 operations in all the nested levels of dict
    """
    for key, value in insight_response.items():
        # converting the key, Camelcase to snake case here
        snake_case_key = re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()

        # if the value is of type dict we will call the function recursively to convert the nested dict keys as well
        if isinstance(value, dict):
            processed_response[snake_case_key] = convert_keys_camel_to_snake_case(dict(), value)

        elif isinstance(value, list):
            if snake_case_key not in processed_response:
                processed_response[snake_case_key] = []

            for item in value:
                # converting nested dict keys with recursion
                if isinstance(item, dict):
                    processed_response[snake_case_key].append(convert_keys_camel_to_snake_case(dict(), item))
                else:
                    processed_response[snake_case_key].append(item)
        else:
            processed_response[snake_case_key] = value

    return processed_response
