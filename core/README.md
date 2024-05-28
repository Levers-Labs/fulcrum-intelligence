Core Engine for Fulcrum Intelligence

## Prerequisites

- `Python 3.10+`
- `Poetry 1.2+`

## Development

Navigate to the core directory and run the following commands:

### Install Poetry

```shell
pip install poetry
```

### Install dependencies

```shell
poetry install
```

## Adding New Analysis Modules

The `BaseAnalyzer` class is an abstract base class that provides a foundation for creating analysis modules.
It includes common functionality such as input validation, data preprocessing, result postprocessing, and value
rounding.
By inheriting from `BaseAnalyzer`, you can easily create new analysis modules that adhere to a consistent structure and
interface.

## Steps to Add a New Analysis Module

1. Create a new Python file for your analysis module in the appropriate directory.

2. Import the necessary dependencies, including the `BaseAnalyzer` class from the `base` module.

3. Define a new class for your analysis module that inherits from `BaseAnalyzer`. For example:

   ```python
   from .base import BaseAnalyzer

   class MyAnalyzer(BaseAnalyzer):
       # Your analysis module implementation

4. Implement the analyze method in your analysis module class. This method should contain the core logic of your
   analysis. It takes a DataFrame (df) as input and returns the analysis result as a dictionary or DataFrame.
   You can also define additional parameters specific to your analysis module.
    ```python
    def analyze(self, df: pd.DataFrame, **kwargs) -> dict[str, Any] | pd.DataFrame:
        # Your analysis logic here
        result = ...
        return result
    ```

5. (Optional) If your analysis module requires specific input validation, you can override the validate_input method
   to perform custom validation checks. Make sure to call the super().validate_input(df) method to include the base
   validation logic.
    ```python
    def validate_input(self, df: pd.DataFrame, **kwargs):
        super().validate_input(df)
        # Your custom validation logic here
    ```

6. (Optional) If your analysis module requires specific data preprocessing, you can override the preprocess_data method
   to perform custom preprocessing steps. Make sure to call the super().preprocess_data(df) method to include the base
   preprocessing logic.
    ```python
    def preprocess_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        df = super().preprocess_data(df)
        # Your custom preprocessing logic here
        return df
    ```

7. (Optional) If your analysis module requires specific postprocessing steps, you can override the post_process_result
   method to perform custom postprocessing. Make sure to call the super().post_process_result(result) method to include
   the base postprocessing logic.
    ```python
    def post_process_result(self, result: dict[str, Any] | pd.DataFrame) -> dict[str, Any] | pd.DataFrame:
        result = super().post_process_result(result)
        # Your custom postprocessing logic here
        return result
    ```

8. Use the run method provided by the BaseAnalyzer class to execute the complete analysis process. This method takes
   care of input validation, data preprocessing, analysis execution, result postprocessing, and value rounding.
    ```python
    codeanalyzer = MyAnalyzer()
    result = analyzer.run(df)
    ```

9. Test your analysis module thoroughly to ensure it produces the expected results and handles different scenarios
   correctly.

10. Add unit tests for your analysis module to cover various input cases and edge cases.
