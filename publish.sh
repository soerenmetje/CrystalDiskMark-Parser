
# SET VERSION IN:
# - setup.py
# - __init__.py

# build library
python3 setup.py sdist bdist_wheel
# publish built library
python3 -m twine upload dist/*