from setuptools import setup, find_packages
import remork

setup(
    name='remork',
    version=remork.VERSION,
    url='https://github.com/baverman/remork/',
    license='MIT',
    author='Anton Bobrov',
    author_email='baverman@gmail.com',
    description='Configuration management and accelerator for testinfra',
    long_description=open('README.md').read(),
    packages=find_packages(exclude=['tests']),
    zip_safe=False,
    platforms='any',
    install_requires=[
        'pytest-testinfra',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        # 'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Operating System :: Unix',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Systems Administration',
    ]
)
