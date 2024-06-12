
REBAR := $(abspath $(shell which ../bin/rebar3 || which rebar3))
SERVICE_NAME=mzbench_api
GIT_HEAD := $(shell git rev-parse HEAD)

SOURCES := $(shell find src dashboard test -name .eunit -prune -o -name node_modules -prune -o -type f -print)
MZBENCH_COMMON_SOURCES := $(wildcard ../common_apps/*/include/*) $(wildcard ../common_apps/*/src/*) $(wildcard ../common_apps/*/rebar.config)
RELEASE_BINARY := _build/default/rel/$(SERVICE_NAME)/bin/$(SERVICE_NAME)

TMP_ROOT:= $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))/tmproot

.PHONY: compile
compile: .make/compilation-up-to-date

.PHONY: console
console: $(RELEASE_BINARY)
	$(RELEASE_BINARY) console

.PHONY: tmp-dir
tmp-dir:
	-rm -rf $(TMP_ROOT)
	mkdir -p $(TMP_ROOT)/opt/mzbench_api/cache

.PHONY: build-workers
build-workers:
	cd ../workers/ && \
	for WORKER in * ; do \
		make -C $${WORKER}/ generate_tgz && \
		cp $${WORKER}/$${WORKER}_worker.tgz $(TMP_ROOT)/opt/mzbench_api/cache/$${WORKER}-someversion-someos.tgz ; \
	done

.PHONY: extract-workers
extract-workers: tmp-dir build-workers
	cd $(TMP_ROOT)/opt/mzbench_api/cache/ && \
	for WORKER_TGZ in * ; do \
		tar xzf $${WORKER_TGZ} -C ~/.local/share/mzbench_workers ; \
	done

.PHONY: rpm
rpm: generate tmp-dir build-workers
	echo "You need to disable custom_os_code_builds and auto_update_deployed_code to make your RPM work"
	cp -R _build/default/rel/mzbench_api $(TMP_ROOT)/opt
	make -C ../node generate_tgz
	cp ../node/node.tgz $(TMP_ROOT)/opt/mzbench_api/cache/node-someversion-someos.tgz
	-git clone https://github.com/flussonic/epm _build/epm
	EPM="-s dir --url http://github.com/machinezone/mzbench --description 'Benchmark server' \
	-m 'Info <info@mz.com>' --vendor 'MZ' --license BSD"
	cd $(TMP_ROOT) && ../_build/epm/epm.erl -f -t deb -n mzbench -v 0.5.8 $(EPM) -a amd64 \
			   --category net opt
	cd $(TMP_ROOT) && ../_build/epm/epm.erl -f -t rpm -n mzbench -v 0.5.8 $(EPM) -a amd64 \
			   --category Server/Benchmark opt

.PHONY: js-dev
js-dev: compile
	ORIG_PATH=$(PATH) erl -pa _build/default/deps/*/ebin -eval 'application:ensure_all_started(mzbench_api)'

.PHONY: jsbuild-auto
jsbuild-auto:
	cd dashboard && npm start

.PHONY: jsbuild-prod
jsbuild-prod:
	cd dashboard && npm run build
	echo $$((`cat priv/version`+1)) > priv/version

.PHONY: test-unit
test-unit: compile
	$(REBAR) eunit skip_deps=true

.PHONY: check
check: test-unit dialyzer.log

.PHONY: clean
clean:
	$(REBAR) clean
	rm -rf _build
	rm -rf .make

.PHONY: generate
generate: $(RELEASE_BINARY) ~/.local/share/mzbench_api/server.crt

~/.local/share/mzbench_api/server.crt:
	mkdir -p ~/.local/share/mzbench_api/
	openssl req -subj '/CN=domain.com/O=My Company Name LTD./C=US' -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout ~/.local/share/mzbench_api/server.key -out ~/.local/share/mzbench_api/server.crt

.make/compilation-up-to-date: rebar.config $(SOURCES) .make/git-rev-$(GIT_HEAD) $(MZBENCH_COMMON_SOURCES)
	$(REBAR) compile

	# make -C _build/default/deps/jiffy # Until https://github.com/davisp/jiffy/issues/96 is resolved

	mkdir -p .make && touch $@

.make/git-rev-$(GIT_HEAD):
	rm -rf .make/git-rev-*
	mkdir -p .make && touch $@

$(RELEASE_BINARY): .make/compilation-up-to-date rel/files/sys.config rel/files/vm.args
	$(REBAR) release
	touch $(RELEASE_BINARY)

BUILD_PLT_FLAGS := --apps deps/cowboy
-include ../lib/dialyzer.mk
