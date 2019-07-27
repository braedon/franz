from setuptools import setup, find_packages

setup(
    name='franz',
    version='0.1.0.dev1',
    description='(Franz) Kafka CLI tool',
    url='https://github.com/Braedon/franz',
    author='Braedon Vickers',
    author_email='braedon.vickers@gmal.com',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Topic :: Utilities',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='kafka cli',
    packages=find_packages(),
    install_requires=[
        'click',
        'crc32c',
        # 'kafka-python @ git+git://github.com/dpkp/kafka-python@51313d792a24059d003f5647ec531cfd9d62d7ab',
        # 'kafka-python @ git+git://github.com/braedon/kafka-python@5a50756132cb328e2c256dc0f6ca896b997ec23a',
        'kafka-python==1.4.6',
        'python-snappy',
        'tonyg-rfc3339',
    ],
    entry_points={
        'console_scripts': [
            'franz=franz:main',
        ],
    },
)
