# Manually build and publish package

# SET VERSION IN:
# - setup.py
# - __init__.py

# build library
# python3 setup.py sdist bdist_wheel
python3 -m build  --sdist --wheel --outdir dist/ .

# publish built library
python3 -m twine upload dist/*