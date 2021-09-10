## Changelog

* [CHANGE] Memberlist: allow specifying address and port advertised to the memberlist cluster members by setting the following configuration (#37):
  * `-memberlist.advertise_addr`
  * `-memberlist.advertise_port`
* [CHANGE] Removed global metrics for KV package. Making a KV object will now require a prometheus registerer that will
  be used to register all relevant KV class metrics. #22
* [CHANGE] Added CHANGELOG.md and Pull Request template to reference the changelog
* [ENHANCEMENT] Add middleware package. #38
