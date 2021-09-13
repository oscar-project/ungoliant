/*!

  # Rebuilding OSCAR

  This module and command enables the rebuilding of the oscar corpus.

  Since OSCAR can be too heavy/too legally touchy to redistribute freely, we provide what's necessary to rebuild corpora following OSCAR Schema.

  This is done by providing language-specific files containing `shard:record:ranges`-like information, named **localization**

  This module has to work for the three following cases:

  1. Generation of rebuilding files:
      1. You have a corpus that follows pre-OSCAR Schema v1.2 (= does not have localization information in metadata)
      2. You have a corpus that follows post-OSCAR Schema v1.2 (= does have localization information in metadata) [^1] (see [here](#generation-of-rebuilding-files-for-oscar-schema--12))
  2. Generation of corpora from rebuilding files:
    - You have a rebuilding file (or many) and you want to rebuild corpora (they'll be in OSCAR Schema v1.2)


  ## Generation of rebuilding files for Oscar Schema < 1.2

  Multiple steps are required to do this:

  (For a given lang)

  1. Read already generated corpus, store a set of all `record_id`.
  2. Create a `HashMap<RecordId, Localization>`
  2. Read CommonCrawl shards.
      1. for each CC record, if record_id is in corpus:
          1. Read lines on CC and corpus, and index corpus in CC (we want to have some [std::ops::Range] that would reconstitute corpus record from CC)
          2. Store that in the HashMap.

  Now, we have something that tells us where to find a given recordid.

  Then:

  1. Read already generated corpus, get corpus' `record_id`, [^2] /!\ : It is possible to have multiple metadata entries for the same record_id!!

  [^1]: We don't (yet) propose a way to convert v1.1 corpora to v1.2.
  [^2]: It may be possible for multiple metadata entries to have same record_ids. It depends on if same record_ids were merged on generation.
        If that's the case, we should see a number of ranges equal to the number of entries.
!*/

mod origin;
mod patch;
mod rebuilder;
pub use rebuilder::prep_rebuild;
