# Compile and test a lab solution, mirroring the Go labs' Makefile.
#
#   $ make lab1            # test Lab 1 and create lab1-handin.tar.gz
#   $ make lab2
#   $ make lab3a           # (or lab3b/lab3c/lab3d)
#   $ make lab4a           # (or lab4b/lab4c)
#   $ make lab5a           # (or lab5b/lab5c)
#
# To run only the tests of a lab, use the suite targets directly:
#
#   $ make mr kvsrv1 lock1 raft1 rsm1 kvraft1 shardkv
#
# Use `RUN=<filter>` to run matching tests only (the analog of the Go
# `make RUN="-run 3A" raft1`; cargo takes a plain substring filter):
#
#   $ make RUN=3A raft1
#   $ make RUN=Wc mr

VERB=--nocapture

SUITE_mr       = cargo test -p mr --test mr_test $(RUN) -- $(VERB)
SUITE_kvsrv1   = cargo test -p kvsrv1 --test kvsrv_test $(RUN) -- $(VERB)
SUITE_lock1    = cargo test -p kvsrv1 --test lock_test $(RUN) -- $(VERB)
SUITE_raft1    = cargo test -p raft1 --test raft_test $(RUN) -- $(VERB)
SUITE_rsm1     = cargo test -p kvraft1 --test rsm_test $(RUN) -- $(VERB)
SUITE_kvraft1  = cargo test -p kvraft1 --test kvraft_test $(RUN) -- $(VERB)
SUITE_shardkv  = cargo test -p shardkv1 --test shardkv_test $(RUN) -- $(VERB) --test-threads=1

all: mr kvsrv1 lock1 raft1 rsm1 kvraft1 shardkv

mr:       ; @$(SUITE_mr)
kvsrv1:   ; @$(SUITE_kvsrv1)
lock1:    ; @$(SUITE_lock1)
raft1:    ; @$(SUITE_raft1)
rsm1:     ; @$(SUITE_rsm1)
kvraft1:  ; @$(SUITE_kvraft1)
shardkv:  ; @$(SUITE_shardkv)

# ---- submission targets (test, then tar up, like the Go Makefile) -----

define HANDIN
@echo "Checking that your submission builds correctly..."
cargo check --workspace --tests || exit 1
$(SUBST)
@echo "Preparing $(1)-handin.tar.gz"
@echo "Tarring up your submission..."
COPYFILE_DISABLE=1 tar cvzf $(1)-handin.tar.gz \
	"--exclude=target" \
	"--exclude=*.tar.gz" \
	Cargo.toml Cargo.lock .cargo Makefile crates
if test `stat -c "%s" "$(1)-handin.tar.gz" 2>/dev/null || stat -f "%z" "$(1)-handin.tar.gz"` -ge 20971520 ; then \
	echo "File exceeds 20MB."; rm $(1)-handin.tar.gz; exit 1; fi
echo "$(1)-handin.tar.gz successfully created. Please upload the tarball manually on Gradescope."
endef

lab1:
	$(call HANDIN,lab1,$(SUITE_mr))

lab2:
	$(call HANDIN,lab2,$(SUITE_kvsrv1) && $(SUITE_lock1))

lab3a:
	$(call HANDIN,lab3a,$(SUITE_raft1))
lab3b:
	$(call HANDIN,lab3b,$(SUITE_raft1))
lab3c:
	$(call HANDIN,lab3c,$(SUITE_raft1))
lab3d:
	$(call HANDIN,lab3d,$(SUITE_raft1))

lab4a:
	$(call HANDIN,lab4a,$(SUITE_rsm1))
lab4b:
	$(call HANDIN,lab4b,$(SUITE_kvraft1))
lab4c:
	$(call HANDIN,lab4c,$(SUITE_kvraft1))

lab5a:
	$(call HANDIN,lab5a,$(SUITE_shardkv))
lab5b:
	$(call HANDIN,lab5b,$(SUITE_shardkv))
lab5c:
	$(call HANDIN,lab5c,$(SUITE_shardkv))

.PHONY: all mr kvsrv1 lock1 raft1 rsm1 kvraft1 shardkv \
	lab1 lab2 lab3a lab3b lab3c lab3d lab4a lab4b lab4c lab5a lab5b lab5c
