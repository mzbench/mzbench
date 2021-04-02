REBAR3 ?= rebar3

.PHONY: all clean compile xref ci

all: compile xref

ci: compile xref

compile:
	$(REBAR3) compile

clean:
	$(REBAR3) clean

clean-all: clean
	rm -rf _build

xref:
	$(REBAR3) xref
