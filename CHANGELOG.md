# Changelog

## [0.1.1](https://github.com/reqfleet/replay/compare/v0.1.0...v0.1.1) (2026-08-27)


### Bug Fixes

* **release:** Publish multi-architecture images ([#110](https://github.com/reqfleet/replay/issues/110)) ([5d26d48](https://github.com/reqfleet/replay/commit/5d26d48c1265aebccc325aabe3dec3e8d3c5dbe6))

## 0.1.0 (2026-08-26)


### ⚠ BREAKING CHANGES

* **replay:** Replay no longer accepts gzip input or the -gzip flag.
* NDJSON events must use timestamp; start_time is rejected.
* **replay:** Separate response events are no longer accepted. Put status, response_headers, and response_body on DownstreamEnd requests.
* **config:** Replace require_override, --require-override, and REPLAY_REQUIRE_OVERRIDE with disallow_recorded_targets, --disallow-recorded-targets, and REPLAY_DISALLOW_RECORDED_TARGETS.

### Features

* **config:** Validate metrics path templates ([#88](https://github.com/reqfleet/replay/issues/88)) ([1464cbb](https://github.com/reqfleet/replay/commit/1464cbb168e8ad6c578d941f48a695a55f40a4f4))
* **format:** Remove NDJSON meta event ([#80](https://github.com/reqfleet/replay/issues/80)) ([514cf15](https://github.com/reqfleet/replay/commit/514cf15c6d4c194f9141852f42329b70188f2854))
* introduce rampup support ([#39](https://github.com/reqfleet/replay/issues/39)) ([4a60f7a](https://github.com/reqfleet/replay/commit/4a60f7afc054036bc66b130145aa3b4f9272c0db))
* make the validation and configuration as public api ([#103](https://github.com/reqfleet/replay/issues/103)) ([43e4e0b](https://github.com/reqfleet/replay/commit/43e4e0b21b66782cf3812c6873ee34aef1a06110))
* **metrics:** Add path templating to reduce label cardinality ([#21](https://github.com/reqfleet/replay/issues/21)) ([00313bd](https://github.com/reqfleet/replay/commit/00313bd89aad4bd09bd027fb9f610698354f1355))
* **metrics:** Limit Prometheus labels to avoid cardinality explosion ([#24](https://github.com/reqfleet/replay/issues/24)) ([1cc3d93](https://github.com/reqfleet/replay/commit/1cc3d935ccbfbc32f08456c49f7394551061ff16))
* mvp ([#2](https://github.com/reqfleet/replay/issues/2)) ([df833c7](https://github.com/reqfleet/replay/commit/df833c7151b3bbcf83a989dc40a0656b91ac1ae9))
* **parser:** add support for reading compressed log files ([#19](https://github.com/reqfleet/replay/issues/19)) ([007f3e8](https://github.com/reqfleet/replay/commit/007f3e8f2a07c07396423c363e02d39089eb7988)), closes [#17](https://github.com/reqfleet/replay/issues/17)
* **replay:** Add Envoy access log combiner ([#102](https://github.com/reqfleet/replay/issues/102)) ([c1cb98c](https://github.com/reqfleet/replay/commit/c1cb98caade56e047b77b1e29e3741094887f63f))
* **replay:** Add verbose mode for detailed logging ([#8](https://github.com/reqfleet/replay/issues/8)) ([97701e3](https://github.com/reqfleet/replay/commit/97701e3907ebaa358be29c95b433427617e8b99e))
* **replay:** Remove gzip input support ([#104](https://github.com/reqfleet/replay/issues/104)) ([894a15b](https://github.com/reqfleet/replay/commit/894a15b32de61b09016e8b459af15494880845f8))
* **tools:** Generate downstream-end request payloads ([#82](https://github.com/reqfleet/replay/issues/82)) ([5e9e1e2](https://github.com/reqfleet/replay/commit/5e9e1e2d79fd84c14c2fffe2e0cfec1bb7e5c8a0))


### Bug Fixes

* a correct replay logic by supporting request-start access logs from Envoy ([#58](https://github.com/reqfleet/replay/issues/58)) ([51511b8](https://github.com/reqfleet/replay/commit/51511b8f6dd213c809769784215528d102261f47))
* add missing skills-lock ([#3](https://github.com/reqfleet/replay/issues/3)) ([9329fc0](https://github.com/reqfleet/replay/commit/9329fc02b681a28627a98464cb96adabe2b1e1f4))
* **api:** fix the log contract ([#89](https://github.com/reqfleet/replay/issues/89)) ([101a1b9](https://github.com/reqfleet/replay/commit/101a1b96803461edfec471561de18fa5ea71ea91))
* better envvar handling ([#33](https://github.com/reqfleet/replay/issues/33)) ([dff8722](https://github.com/reqfleet/replay/commit/dff8722cdcacd62fdd5b1aac868eea7137064b41))
* **config:** Enforce lowercase authority pseudo-header ([#71](https://github.com/reqfleet/replay/issues/71)) ([218f8cc](https://github.com/reqfleet/replay/commit/218f8cc240c3a8200b361e90d4f52c060cc3aad7))
* **config:** Reject invalid required target overrides ([#66](https://github.com/reqfleet/replay/issues/66)) ([b631b10](https://github.com/reqfleet/replay/commit/b631b103f1149a03fe071ffcda28bff5d4e04db3))
* **config:** Remove obsolete env map ([#78](https://github.com/reqfleet/replay/issues/78)) ([aa576df](https://github.com/reqfleet/replay/commit/aa576dffa88beb485fe2c5aaa173f198331740b4))
* **config:** Remove unused connection cap ([#101](https://github.com/reqfleet/replay/issues/101)) ([1edbd18](https://github.com/reqfleet/replay/commit/1edbd18c8949bbdc57b849b1ad65af8ea7dc9c61)), closes [#37](https://github.com/reqfleet/replay/issues/37)
* core replay logic was problematic ([#50](https://github.com/reqfleet/replay/issues/50)) ([50c69a9](https://github.com/reqfleet/replay/commit/50c69a91181ba2ce2fa6c4fae27c9d13ed6e7412))
* **docs:** add missing path_template in the docs ([#87](https://github.com/reqfleet/replay/issues/87)) ([a365bcc](https://github.com/reqfleet/replay/commit/a365bcc6357d772965f0575053a40fded65c4a07))
* during replay, when target is not reachable, we should not fail the process itself ([61e3d1e](https://github.com/reqfleet/replay/commit/61e3d1e0be44db9df61ead1a42e01c0a51833eaa))
* **engine:** Account for elapsed time when pacing requests ([#91](https://github.com/reqfleet/replay/issues/91)) ([ee2f5dc](https://github.com/reqfleet/replay/commit/ee2f5dca079112c9a445112c2573dac4057943e6))
* **engine:** Close checkpoints and stop aborted replays ([#5](https://github.com/reqfleet/replay/issues/5)) ([14a1841](https://github.com/reqfleet/replay/commit/14a18417d681968a3ab6e056f5a61367ffddfb5a))
* **engine:** ignore HTTP/2 pseudo-headers when setting request headers ([#9](https://github.com/reqfleet/replay/issues/9)) ([f2abcca](https://github.com/reqfleet/replay/commit/f2abcca9c132cf3e38517f7591df7a74b1c9f0f3))
* fix issues reported by gopls ([#51](https://github.com/reqfleet/replay/issues/51)) ([cd43cca](https://github.com/reqfleet/replay/commit/cd43ccaeb7b619f632c87b01d06b4f0056e5a129))
* make it work with actual Envoy access log ([#7](https://github.com/reqfleet/replay/issues/7)) ([4793c99](https://github.com/reqfleet/replay/commit/4793c99dbe7e568dd0dc543e55ff0806c0175dbe))
* **metrics:** Make Replay metrics neutral ([#53](https://github.com/reqfleet/replay/issues/53)) ([abe1e87](https://github.com/reqfleet/replay/commit/abe1e87ae65a51e05d07c715b38980660ef70998))
* **metrics:** Prefer cgroup runtime stats ([#55](https://github.com/reqfleet/replay/issues/55)) ([cc74875](https://github.com/reqfleet/replay/commit/cc74875d428684712fe5c66e61a253fb0619528b))
* **metrics:** Strip query string from label to prevent cardinality explosion ([#20](https://github.com/reqfleet/replay/issues/20)) ([43ec625](https://github.com/reqfleet/replay/commit/43ec625b6d2e28b322d0a886d57be51b51f595cc))
* **metrics:** Track active VUs by connection lifecycle ([#59](https://github.com/reqfleet/replay/issues/59)) ([b35c1fe](https://github.com/reqfleet/replay/commit/b35c1fece168351b4fe9675a1f25d7f3e01410da))
* **parser:** make meta event optional ([#12](https://github.com/reqfleet/replay/issues/12)) ([398abde](https://github.com/reqfleet/replay/commit/398abde52384fc344f3590ef185e5b7bbf3d8a6e))
* **prom:** add some delays to the metric endpoint during shutdown to … ([#34](https://github.com/reqfleet/replay/issues/34)) ([b22c5ac](https://github.com/reqfleet/replay/commit/b22c5acb17fcb27e1fe48ae7a1e725a66e84cd22))
* remaining p3 issues ([#69](https://github.com/reqfleet/replay/issues/69)) ([0ae309e](https://github.com/reqfleet/replay/commit/0ae309eea1a55f958d421b31bbe1c8c5bf87e747))
* **replay:** Apply header rewrites before idempotency checks ([#67](https://github.com/reqfleet/replay/issues/67)) ([2610df2](https://github.com/reqfleet/replay/commit/2610df2e03c8e0f9024146cf61132f5b40244b33))
* **replay:** fix remaining issues  ([#68](https://github.com/reqfleet/replay/issues/68)) ([67d2eb7](https://github.com/reqfleet/replay/commit/67d2eb756fb50c48e425fe9750480c4b4d10de85))
* **replay:** Group connections by node and connection ID ([#32](https://github.com/reqfleet/replay/issues/32)) ([c53f36f](https://github.com/reqfleet/replay/commit/c53f36fdad6d6d3261490bf0e12d2241c3ab37e3))
* **replay:** Preserve recorded connection sockets ([#65](https://github.com/reqfleet/replay/issues/65)) ([e6832c9](https://github.com/reqfleet/replay/commit/e6832c957c84cc87f611278bd1527ffb28ec4e26))
* restore the vu semantic ([#60](https://github.com/reqfleet/replay/issues/60)) ([7556d89](https://github.com/reqfleet/replay/commit/7556d891af2a8cfd6caed9f2131fadf236692e81))
* **skills:** fix the commit author name ([#23](https://github.com/reqfleet/replay/issues/23)) ([ddab4ec](https://github.com/reqfleet/replay/commit/ddab4ecaa7e15add841c0b7abc517231359e70fb))
* the client behaviour should expect what the logs present ([#42](https://github.com/reqfleet/replay/issues/42)) ([55b3f5a](https://github.com/reqfleet/replay/commit/55b3f5ae4799a98c1429b5db287f32d5760d9622))
* threads count should not be goroutine count ([#36](https://github.com/reqfleet/replay/issues/36)) ([47f8543](https://github.com/reqfleet/replay/commit/47f8543efb32705b6be1b678411faa82fe4774ed))


### Performance Improvements

* **checkpoint:** Batch progress persistence ([#85](https://github.com/reqfleet/replay/issues/85)) ([e4055e4](https://github.com/reqfleet/replay/commit/e4055e495d4b75874731ff627d6f4b1eb50813ad))
* **checkpoint:** Coalesce durable snapshot writes ([#72](https://github.com/reqfleet/replay/issues/72)) ([076bbd2](https://github.com/reqfleet/replay/commit/076bbd29b02587e16f890598ededc8d066fa9f15))
* **engine:** Optimize request URL building and target URL rewriting  ([#45](https://github.com/reqfleet/replay/issues/45)) ([77c600b](https://github.com/reqfleet/replay/commit/77c600bc4e455a41b9f00e4f4b9ab270e791509b))
* **engine:** Remove redundant stream sorting in groupRequestsByStream ([#47](https://github.com/reqfleet/replay/issues/47)) ([eea346c](https://github.com/reqfleet/replay/commit/eea346ceebbd22dc27c1c283d397f97e82c6fc13))
* make checkpoint faster ([#52](https://github.com/reqfleet/replay/issues/52)) ([53352e3](https://github.com/reqfleet/replay/commit/53352e3716ab44bf3177a49bb012fdb3be2b23e0))
* make handling responses faster ([#73](https://github.com/reqfleet/replay/issues/73)) ([4909422](https://github.com/reqfleet/replay/commit/49094229523011c49d93f607714089aade3768de))
* **replay:** Avoid unused response validation state ([#74](https://github.com/reqfleet/replay/issues/74)) ([be98698](https://github.com/reqfleet/replay/commit/be9869835941a56b6ca33e73201bac01e30704d2))
* **replay:** Reduce JSON and header allocation costs ([#108](https://github.com/reqfleet/replay/issues/108)) ([f396414](https://github.com/reqfleet/replay/commit/f396414213f26818c2834de543bef63c090b3c21))
* **replay:** Validate Envoy responses inline ([#84](https://github.com/reqfleet/replay/issues/84)) ([e8f9f21](https://github.com/reqfleet/replay/commit/e8f9f21e09b07d322f9d8c9b1ae03b70cd6f0928))


### Miscellaneous Chores

* add an example config for envoy recording ([#90](https://github.com/reqfleet/replay/issues/90)) ([ae70cc0](https://github.com/reqfleet/replay/commit/ae70cc01bbbf708eb2d77956e57304f9f210a0cb))
