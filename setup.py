from distutils.core import setup
from os.path import dirname, join, realpath

__TOP_LEVEL_DIR = dirname(realpath(__file__))

with open(join(__TOP_LEVEL_DIR, 'requirement.txt')) as f:
    dependencies = [r.strip() for r in f]

setup(
    name='basics',
    version='1.2',
    packages=('utility', 'stream'),
    url='https://bitbucket.org/ShivKJ/basics/',
    license='SKJ',
    author='Shiv',
    author_email='None@gmail.com',
    description='basic utility functions',
    install_requires=dependencies,
    python_requires='>=3.6',
    platforms='ubuntu'
)
