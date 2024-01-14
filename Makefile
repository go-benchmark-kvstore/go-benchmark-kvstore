SHELL = /bin/bash -o pipefail

.PHONY: benchmark plot lint lint-ci fmt fmt-ci clean lint-docs audit encrypt decrypt sops

benchmark:
	go run ./... benchmark "--logging.file.path=$(ENGINE).log"

plot:
	go run ./... plot *.log

lint:
	golangci-lint run --timeout 4m --color always --allow-parallel-runners --fix

lint-ci:
	golangci-lint run --timeout 4m --out-format colored-line-number,code-climate:codeclimate.json

fmt:
	go mod tidy
	git ls-files --cached --modified --other --exclude-standard -z | grep -z -Z '.go$$' | xargs -0 gofumpt -w
	git ls-files --cached --modified --other --exclude-standard -z | grep -z -Z '.go$$' | xargs -0 goimports -w -local gitlab.com/go-benchmark-kvstore/go-benchmark-kvstore.gitlab.io

fmt-ci: fmt
	git diff --exit-code --color=always

clean:
	rm -f coverage.* codeclimate.json tests.xml

lint-docs:
	npx --yes --package 'markdownlint-cli@~0.34.0' -- markdownlint --ignore-path .gitignore --ignore testdata/ '**/*.md'

audit:
	go list -json -deps ./... | nancy sleuth --skip-update-check

encrypt:
	gitlab-config sops --encrypt --mac-only-encrypted --in-place --encrypted-comment-regex sops:enc .gitlab-conf.yml

decrypt:
	SOPS_AGE_KEY_FILE=keys.txt gitlab-config sops --decrypt --in-place .gitlab-conf.yml

sops:
	SOPS_AGE_KEY_FILE=keys.txt gitlab-config sops .gitlab-conf.yml