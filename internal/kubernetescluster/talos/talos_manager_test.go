package talos

import (
	"testing"
)

func TestSplitYAMLDocuments_SingleDocument(t *testing.T) {
	input := "machine:\n  sysctls:\n    user.max_user_namespaces: \"11255\"\n  network:\n    nameservers:\n      - 10.246.196.76"

	docs := splitYAMLDocuments(input)
	if len(docs) != 1 {
		t.Fatalf("expected 1 document, got %d", len(docs))
	}
	if docs[0] != input {
		t.Errorf("unexpected content:\n%s", docs[0])
	}
}

func TestSplitYAMLDocuments_MultipleDocuments(t *testing.T) {
	input := "machine:\n  sysctls:\n    user.max_user_namespaces: \"11255\"\n---\napiVersion: v1alpha1\nkind: ResolverConfig\ndnsServers:\n  - 10.246.196.76\n---\napiVersion: v1alpha1\nkind: TimeConfig\ndisabled: false\nservers:\n  - ntp.nhn.no"

	docs := splitYAMLDocuments(input)
	if len(docs) != 3 {
		t.Fatalf("expected 3 documents, got %d", len(docs))
	}

	// First doc should be the machine config
	expected0 := "machine:\n  sysctls:\n    user.max_user_namespaces: \"11255\""
	if docs[0] != expected0 {
		t.Errorf("unexpected first document:\ngot:  %q\nwant: %q", docs[0], expected0)
	}

	// Second doc should be the ResolverConfig
	expected1 := "apiVersion: v1alpha1\nkind: ResolverConfig\ndnsServers:\n  - 10.246.196.76"
	if docs[1] != expected1 {
		t.Errorf("unexpected second document:\ngot:  %q\nwant: %q", docs[1], expected1)
	}

	// Third doc should be the TimeConfig
	expected2 := "apiVersion: v1alpha1\nkind: TimeConfig\ndisabled: false\nservers:\n  - ntp.nhn.no"
	if docs[2] != expected2 {
		t.Errorf("unexpected third document:\ngot:  %q\nwant: %q", docs[2], expected2)
	}
}

func TestSplitYAMLDocuments_EmptyInput(t *testing.T) {
	docs := splitYAMLDocuments("")
	if len(docs) != 0 {
		t.Fatalf("expected 0 documents, got %d", len(docs))
	}
}

func TestSplitYAMLDocuments_OnlySeparators(t *testing.T) {
	docs := splitYAMLDocuments("---\n---\n---")
	if len(docs) != 0 {
		t.Fatalf("expected 0 documents, got %d", len(docs))
	}
}

func TestSplitYAMLDocuments_LeadingSeparator(t *testing.T) {
	input := "---\nmachine:\n  network:\n    nameservers:\n      - 10.0.0.1\n---\napiVersion: v1alpha1\nkind: TimeConfig\ndisabled: false"

	docs := splitYAMLDocuments(input)
	if len(docs) != 2 {
		t.Fatalf("expected 2 documents, got %d", len(docs))
	}
}

func TestSplitYAMLDocuments_TrailingWhitespace(t *testing.T) {
	input := "machine:\n  install:\n    disk: /dev/sda\n---\napiVersion: v1alpha1\nkind: ResolverConfig\ndnsServers:\n  - 8.8.8.8\n"

	docs := splitYAMLDocuments(input)
	if len(docs) != 2 {
		t.Fatalf("expected 2 documents, got %d", len(docs))
	}
}

func TestSplitYAMLDocuments_EmptyDocumentsBetweenSeparators(t *testing.T) {
	input := "machine:\n  install:\n    disk: /dev/sda\n---\n\n---\napiVersion: v1alpha1\nkind: ResolverConfig\ndnsServers:\n  - 8.8.8.8"

	docs := splitYAMLDocuments(input)
	if len(docs) != 2 {
		t.Fatalf("expected 2 documents (empty docs skipped), got %d", len(docs))
	}
}
