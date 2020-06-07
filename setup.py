from os.path import dirname, join, realpath

from setuptools import find_packages, setup

__TOP_LEVEL_DIR = dirname(realpath(__file__))

with open(join(__TOP_LEVEL_DIR, 'requirements.txt')) as f:
    dependencies = tuple(pkg.strip() for pkg in f)

setup(
    name='streamAPI',
    version='2.0.5',
    packages=tuple(pkg for pkg in find_packages(__TOP_LEVEL_DIR) if 'test.' not in pkg),
    url='https://github.com/ShivKJ/Stream',
    license='MIT License',
    author='Shiv',
    author_email='shivkj001@gmail.com',
    description='basics utility and stream processing functionality',
    long_description='basics utility and stream processing functionality',
    install_requires=dependencies,
    python_requires='>=3.6',
    platforms='ubuntu',
    classifiers=(
        'Programming Language :: Python :: 3.6',
        'License :: OSI Approved :: MIT License',
        'Topic :: Software Development :: Libraries :: Python Modules'
    )
)
