* There are other parts that aren't ready for public consumption yet,
that implement tag grouping, tag suggestion/correlation, and docstrings
for tags. All of these are fairly experimental (as is aliasing) and are
mostly intended to assist tag completion /GUIs. I'm currently considering
whether to expand docstring support to encompass tag groups as well.

* My existing 'tag' tagging gui will probably also go in, hopefully with
shelling-out to tmsu reduced to a minimum.

* Other tmsu-related tools and affordances, like:
  * sxiv keyhandler snippets related to tagging
  * Thunar custom action for tagging
  * (yet to be written) Thunar select-by-tag-query plugin (eg files with specific tag, files with >N tags ..).
    Limited to (non-recursive) contents of the current directory, for usability purposes
  * sxiv image-info snippet for displaying tmsu tags quickly
  * Systems for importing taggings
  * Systems for monitoring directories and prompting the user to tag new files that are saved there
  * 'existingtags' -- accept a list of tags, output only the ones TMSU already knows.
  * mvandtag -- move files to a new directory (and optionally tag them), ensuring that TMSU continues to track them correctly.
  * Extended querying and filtering of an existing fileset (tagfilt)
  * tmsq -- query result cacher
  * waitfortmsu -- wait until the lock on the TMSU database is released. For use before an operation that needs to write to the DB -- eg tmsu tag.
  * These all exist already in some form, though they need polish.

  (although I may end up putting these in a separate repository)