# Optibus Duty Processor

## Overview
Optibus Duty Processor is a command-line application built with Python that processes duty-related files. The application can be executed with specific parameters to define the file to be processed and the operations to be performed.

## Installation
Ensure you have Python installed on your system. Clone or download the application repository and navigate to its root directory.

## Usage
To run the application, execute the following command:
```sh
python -m app --filename <file> --operations <operations>
```

### Arguments
- `--filename` (Required): Specifies the file to be loaded for processing.
- `--operations` (Required): Specifies the operations to execute. Possible values:
  - `a`: Execute all available operations.
  - `b`: Process breaks.
  - `s`: Process start and end times.
  - `n`: Process start and end times with stop names.

## Environment Variables
The application relies on an `.env` file to define paths for file ingestion and output.

| Variable Name         | Default Value  | Description |
|----------------------|---------------|-------------|
| `FILE_INGESTION_PATH` | `/ingestion/` | Specifies the directory where input files are located. |
| `FILE_OUTPUT_PATH`    | `/output/`    | Specifies the directory where processed files are saved. |
| `USE_RELATIVE_PATH`    | `True`    | Specifies wether the provided path is absolute or relative (default). |

If not provided, the application will use the default values.

## Example Usage
```sh
python -m app --filename duties.json --operations a
```
This will process the file `duties.json` using all available operations.

```sh
python -m app --filename schedule.json --operations s
```
This will process the file `schedule.json`, extracting only start and end times.

## License
This project is licensed under the MIT License.

## Contact
For any questions or issues, please reach out to the project maintainers.

