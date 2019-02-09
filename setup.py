try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

packages = [
    'aio_periodic',
]

requires = ['aio_periodic']

setup(
    name='aio_periodic',
    version='0.1.8',
    description='The periodic task system client for python3 base on asyncio',
    author='Li Meng Jun',
    author_email='lmjubuntu@gmail.com',
    url='https://github.com/Lupino/python-aio-periodic',
    packages=packages,
    package_dir={'aio_periodic': 'aio_periodic'},
    include_package_data=True,
    install_requires=requires,
)
