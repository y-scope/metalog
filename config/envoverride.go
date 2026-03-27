package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
)

// ApplyEnvOverrides walks a config struct and overrides fields that have an
// `env:"VAR_NAME"` tag with the corresponding environment variable value.
//
// Struct fields may carry an `envprefix:"PREFIX_"` tag to prepend a prefix
// when recursing into child structs. This allows the same struct type
// (e.g., DatabaseConfig) to resolve to different env var names depending
// on where it appears in the config hierarchy.
//
// Only non-empty env var values trigger an override — unset or empty vars
// are ignored. Supported field types: string, int, bool.
func ApplyEnvOverrides(cfg any) error {
	return applyEnvOverrides(reflect.ValueOf(cfg), "")
}

func applyEnvOverrides(v reflect.Value, prefix string) error {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil
	}

	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fv := v.Field(i)

		if !field.IsExported() {
			continue
		}

		childPrefix := prefix
		if ep := field.Tag.Get("envprefix"); ep != "" {
			childPrefix = prefix + ep
		}

		// Recurse into struct fields.
		if fv.Kind() == reflect.Struct {
			if err := applyEnvOverrides(fv, childPrefix); err != nil {
				return err
			}
			continue
		}
		if fv.Kind() == reflect.Ptr && fv.Type().Elem().Kind() == reflect.Struct {
			if !fv.IsNil() {
				if err := applyEnvOverrides(fv, childPrefix); err != nil {
					return err
				}
			}
			continue
		}

		// Leaf field with env tag.
		envKey := field.Tag.Get("env")
		if envKey == "" {
			continue
		}
		fullKey := prefix + envKey
		val, ok := os.LookupEnv(fullKey)
		if !ok || val == "" {
			continue
		}

		switch fv.Kind() {
		case reflect.String:
			fv.SetString(val)
		case reflect.Int, reflect.Int64:
			n, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf("env %s: invalid int %q: %w", fullKey, val, err)
			}
			fv.SetInt(int64(n))
		case reflect.Bool:
			b, err := strconv.ParseBool(val)
			if err != nil {
				return fmt.Errorf("env %s: invalid bool %q: %w", fullKey, val, err)
			}
			fv.SetBool(b)
		}
	}
	return nil
}
