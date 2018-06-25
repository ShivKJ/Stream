from os.path import dirname, join, realpath

from setuptools import setup

__TOP_LEVEL_DIR = dirname(realpath(__file__))

with open(join(__TOP_LEVEL_DIR, 'requirement.txt')) as f:
    dependencies = [pkg.strip() for pkg in f]

setup(
    name='streamAPI',
<<<<<<< HEAD
    version='1.3.2.1',
    packages=('streamAPI', 'streamAPI.stream', 'streamAPI.utility'),
=======
    version='1.3.2',
    packages=('streamAPI.stream', 'streamAPI.utility'),
>>>>>>> c3975ed403bbf10ea63ab24c8bea6677845a3a65
    url='https://github.com/ShivKJ/Basics',
    license='MIT License',
    author='Shiv',
    author_email='shivkj001@gmail.com',
    description='basics utility and stream processing functionality',
    long_description="basics utility and stream processing functionality",
    install_requires=dependencies,
    python_requires='>=3.6',
    platforms='ubuntu',
    classifiers=(
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        'Topic :: Software Development :: Libraries :: Python Modules'
    )
)
