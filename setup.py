from setuptools import setup

packages = ['aio_periodic', 'aio_periodic.types']

# Dependencies
# Note: 'asyncio' is part of the stdlib in Python 3 and is removed here.
requires = [
    'asyncio-pool',
    'cryptography'
]

setup(
    name='aio_periodic',
    version='1.0.0',
    description='The periodic task system client for python3 based on asyncio',
    author='Li Meng Jun',
    author_email='lmjubuntu@gmail.com',
    url='https://github.com/Lupino/python-aio-periodic',
    packages=packages,
    package_dir={'aio_periodic': 'aio_periodic'},
    include_package_data=True,
    install_requires=requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
