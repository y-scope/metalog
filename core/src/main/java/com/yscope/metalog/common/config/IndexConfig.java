package com.yscope.metalog.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * YAML configuration model for dynamic index management.
 *
 * <p>Example index.yaml:
 *
 * <pre>
 * indexes:
 *   - name: idx_dim_application_id
 *     columns:
 *       - dim_f01
 *     enabled: true
 *
 *   - name: idx_dim_service_name
 *     columns:
 *       - dim_f02
 *     enabled: false
 *
 *   - name: idx_composite_app_time
 *     columns:
 *       - dim_f01
 *       - min_timestamp
 *     enabled: true
 *
 *   - name: idx_state_expires
 *     columns:
 *       - state
 *       - expires_at
 *     enabled: true
 * </pre>
 */
public class IndexConfig {

  @JsonProperty("indexes")
  private List<IndexDefinition> indexes = new ArrayList<>();

  public List<IndexDefinition> getIndexes() {
    return indexes;
  }

  public void setIndexes(List<IndexDefinition> indexes) {
    this.indexes = indexes;
  }

  /** Find an index by name. */
  public IndexDefinition findByName(String name) {
    return indexes.stream().filter(i -> i.getName().equals(name)).findFirst().orElse(null);
  }

  /** Get all enabled indexes. */
  public List<IndexDefinition> getEnabledIndexes() {
    List<IndexDefinition> enabled = new ArrayList<>();
    for (IndexDefinition idx : indexes) {
      if (idx.isEnabled()) {
        enabled.add(idx);
      }
    }
    return enabled;
  }

  /** Get all disabled indexes. */
  public List<IndexDefinition> getDisabledIndexes() {
    List<IndexDefinition> disabled = new ArrayList<>();
    for (IndexDefinition idx : indexes) {
      if (!idx.isEnabled()) {
        disabled.add(idx);
      }
    }
    return disabled;
  }

  /** Individual index definition. */
  public static class IndexDefinition {
    private String name;
    private List<String> columns = new ArrayList<>();
    private boolean enabled = true;
    private boolean unique = false;
    private IndexType type = IndexType.BTREE;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public List<String> getColumns() {
      return columns;
    }

    public void setColumns(List<String> columns) {
      this.columns = columns;
    }

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public boolean isUnique() {
      return unique;
    }

    public void setUnique(boolean unique) {
      this.unique = unique;
    }

    public IndexType getType() {
      return type;
    }

    public void setType(IndexType type) {
      this.type = type;
    }

    /** Generate the column list for CREATE INDEX statement. */
    public String getColumnList() {
      return String.join(", ", columns);
    }
  }

  /** Index type for MySQL. */
  public enum IndexType {
    BTREE,
    HASH,
    FULLTEXT
  }
}
