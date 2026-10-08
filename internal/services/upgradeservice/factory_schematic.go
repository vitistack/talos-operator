package upgradeservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// factorySchematicTimeout bounds one schematic lookup, so an unreachable image
// factory cannot stall the reconcile.
const factorySchematicTimeout = 10 * time.Second

// maxSchematicBody caps the schematic document read from the factory. Real
// ones are a few hundred bytes.
const maxSchematicBody = 1 << 20

// errSchematicNotFound means the image factory does not know the schematic ID:
// the pin is not a factory image this operator can read.
var errSchematicNotFound = errors.New("schematic not found")

// factorySchematic is what the image factory reports for a schematic ID.
type factorySchematic struct {
	// Extensions are the schematic's official system extensions, e.g.
	// "siderolabs/nfs-utils".
	Extensions []string
	// Customized is true when the schematic carries anything besides system
	// extensions (kernel arguments, META values, an overlay, ...), which the
	// configured schematic would drop.
	Customized bool
}

// schematicLookup returns the image factory's description of the schematic in
// ref, a <registry>/<platform>-installer/<schematic>[:tag] image reference.
type schematicLookup func(ctx context.Context, ref string) (factorySchematic, error)

// lookupFactorySchematic asks the image factory that serves ref's registry for
// the schematic. A schematic ID is a hash of its content, so answers (found or
// not) are cached for the life of the process.
func (s *UpgradeService) lookupFactorySchematic(ctx context.Context, ref string) (factorySchematic, error) {
	repo := imageRepository(ref)
	registry, _, _ := strings.Cut(repo, "/")
	id := schematicID(ref)
	key := registry + "/" + id
	if cached, ok := s.schematics.Load(key); ok {
		if cached == nil {
			return factorySchematic{}, errSchematicNotFound
		}
		return cached.(factorySchematic), nil
	}

	ctx, cancel := context.WithTimeout(ctx, factorySchematicTimeout)
	defer cancel()
	schematic, err := fetchFactorySchematic(ctx, http.DefaultClient, "https://"+registry, id)
	switch {
	case err == nil:
		s.schematics.Store(key, schematic)
	case errors.Is(err, errSchematicNotFound):
		s.schematics.Store(key, nil)
	}
	return schematic, err
}

// fetchFactorySchematic reads GET <baseURL>/schematics/<id> from an image
// factory. Returns errSchematicNotFound on 404; any other failure is
// transient.
func fetchFactorySchematic(ctx context.Context, hc *http.Client, baseURL, id string) (factorySchematic, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/schematics/"+url.PathEscape(id), http.NoBody)
	if err != nil {
		return factorySchematic{}, fmt.Errorf("failed to build schematic request: %w", err)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return factorySchematic{}, fmt.Errorf("failed to fetch schematic %s: %w", id, err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return factorySchematic{}, fmt.Errorf("%w: %s", errSchematicNotFound, id)
	default:
		return factorySchematic{}, fmt.Errorf("failed to fetch schematic %s: %s", id, resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSchematicBody))
	if err != nil {
		return factorySchematic{}, fmt.Errorf("failed to read schematic %s: %w", id, err)
	}
	return parseFactorySchematic(body)
}

// parseFactorySchematic decodes a schematic document:
//
//	customization:
//	    systemExtensions:
//	        officialExtensions:
//	            - siderolabs/iscsi-tools
func parseFactorySchematic(body []byte) (factorySchematic, error) {
	var doc map[string]any
	if err := yaml.Unmarshal(body, &doc); err != nil {
		return factorySchematic{}, fmt.Errorf("failed to parse schematic: %w", err)
	}

	var out factorySchematic
	for key, value := range doc {
		if key != "customization" {
			out.Customized = out.Customized || nonEmpty(value)
			continue
		}
		customization, _ := value.(map[string]any)
		for ckey, cvalue := range customization {
			if ckey != "systemExtensions" {
				out.Customized = out.Customized || nonEmpty(cvalue)
				continue
			}
			systemExtensions, _ := cvalue.(map[string]any)
			for skey, svalue := range systemExtensions {
				if skey != "officialExtensions" {
					out.Customized = out.Customized || nonEmpty(svalue)
					continue
				}
				list, _ := svalue.([]any)
				for _, e := range list {
					if name, ok := e.(string); ok && name != "" {
						out.Extensions = append(out.Extensions, name)
					}
				}
			}
		}
	}
	return out, nil
}

// nonEmpty reports whether a decoded YAML value carries anything.
func nonEmpty(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case map[string]any:
		return len(t) > 0
	case []any:
		return len(t) > 0
	case string:
		return t != ""
	case bool:
		return t
	default:
		return true
	}
}
