from setuptools import setup, find_packages

setup(
    name='dbf-sync-refactor',
    version='2.0.0',
    author='Ryner Lute',
    description='Утилита синхронизации DBF-файлов с БД (SQLite / PostgreSQL) с поддержкой временных рядов',
    long_description=open('README.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/example/dbf-sync-refactor',
    license='MIT',
    packages=find_packages(exclude=['tests*']),
    python_requires='>=3.9',
    install_requires=[
        'pyyaml>=6.0',
        'dbfread>=2.0.7',
        'psycopg2-binary>=2.9.9',
        'tqdm>=4.66.0',
    ],
    entry_points={
        'console_scripts': [
            'dbf-sync=dbf_sync_refactor:main',
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)