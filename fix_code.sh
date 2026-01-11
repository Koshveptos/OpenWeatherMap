
autoflake --in-place --remove-all-unused-imports --remove-unused-variables --ignore-init-module-imports src/*.py


black src/ --line-length 120


isort src/ --profile black


flake8 src/ --max-line-length=120 --extend-ignore=E402,E501,E265,E262
