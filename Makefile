export ERLANG_MK ?= $(CURDIR)/erlang.mk

PROJECT = agency
PROJECT_DESCRIPTION = The ultimate user system
PROJECT_VERSION = 0.0.0

DEPS         = erlkit erloom jfdb
dep_erlkit   = git git@github.com:jflatow/erlkit.git
dep_erloom   = git git@github.com:jflatow/erloom.git
dep_jfdb     = git git@github.com:jflatow/jfdb.git
ERL_LIBS     = $(DEPS_DIR)/jfdb/erlang

all:: $(ERLANG_MK)
$(ERLANG_MK):
	curl https://erlang.mk/erlang.mk | make -f -

include $(ERLANG_MK)
