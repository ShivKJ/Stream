from os.path import dirname, join, realpath

from setuptools import setup

__TOP_LEVEL_DIR = dirname(realpath(__file__))

with open(join(__TOP_LEVEL_DIR, 'requirement.txt')) as f:
    dependencies = [pkg.strip() for pkg in f]

setup(
    name='streamAPI',
    version='1.7',
    packages=('streamAPI', 'streamAPI.stream', 'streamAPI.utility'),
    url='https://github.com/ShivKJ/Basics',
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
