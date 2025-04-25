from setuptools import setup, find_packages

setup(
    name='data-service',
    version='0.1.0',
    description='Cyberbuild Trade Data Service',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[],  # Add your dependencies here
    python_requires='>=3.8',
)
