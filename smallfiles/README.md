# Python CPS-Dataset-Comparison

> This module is not yet implemented.

This is python implementation of the project. It is used for comparing small files (files fitting into RAM).

- [Create and run environment](#create-and-run-environment)
- [Run main](#run-main)
    - [Requirements](#requirements)
- [How to run tests](#how-to-run-tests)

## Create and run environment

```bash
cd smallfiles
python3 -m venv venv
source venv/bin/activate
```

## Run main

> You have to be in smallfiles folder and in activated environment

```bash
python main.py
```

### Requirements

Before running, you should install requirements.
```bash
pip install -r requirements.txt
```

### How to run tests

> You have to be in smallfiles folder and in activated environment
    
```bash
pytest
```

For specific test
```bash
# pytest name_of_test_file.py
 pytest test/test_version.py
```

## Quality gates

| Tool | Command | Gate |
|------|---------|------|
| pytest | `pytest --cov=. --cov-fail-under=80` | coverage >= 80% |
| pylint | `pylint $(git ls-files '*.py')` | score >= 9.5 |
| black | `black --check $(git ls-files '*.py')` | formatting enforced |
| mypy | `mypy .` | type checking enforced |
