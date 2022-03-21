from setuptools import setup, find_packages

setup(
    name='rsocket',
    version='0.3.dev1',
    description='Python RSocket library',
    url='https://github.com/rsocket/rsocket-py',
    author='Gabriel Shaar',
    author_email='gabis@precog.co',
    license='MIT',
    packages=find_packages(exclude=['examples', 'tests', 'docs']),
    zip_safe=True,
    python_requires='>=3.8',
    extra_required={
        'rx': {'Rx >= 3.0.0'},
        'aiohttp': {'aiohttp == 3.8.1'},
        'quart': {'quart == 0.16.2'}
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Environment :: Web Environment',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ])
