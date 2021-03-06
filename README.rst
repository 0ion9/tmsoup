===============================
Tmsoup
===============================

.. image:: https://badge.fury.io/py/tmsoup.png
    :target: http://badge.fury.io/py/tmsoup

.. image:: https://travis-ci.org/0ion9/tmsoup.png?branch=master
        :target: https://travis-ci.org/0ion9/tmsoup

.. image:: https://pypip.in/d/tmsoup/badge.png
        :target: https://pypi.python.org/pypi/tmsoup


Python extensions and enhancements to TMSU

* Free software: LGPLv3 license
* Documentation: https://tmsoup.readthedocs.org... When I set it up.

Features
--------

* alias : API and CLI extending TMSU with aliases.
          An alias maps a name to one or more taggings.
 * Example aliasing uses :
  * Quicker tagging : `fruit_salad` -> `apple banana orange`
  * Easier tagging : `music` -> `𝅘𝅥𝅮`
  * Tag=value shorthand : `2014` -> `year=2014`
  * Modify or simplify taggings as you import them : `soft rock`, `hard rock`, `progressive_rock` -> `rock`

 * For now, the easiest way to use the CLI is:
  * Install tmsoup, via `python3 setup.py install`
  * Create a shell alias 'talias' as follows: `alias talias='python3 -m tmsoup.alias'`
  * Use the alias.

* TODO
