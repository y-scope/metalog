package com.yscope.clp.service.coordinator.ingestion.record;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the name used to look up a {@link RecordTransformer} implementation.
 *
 * <p>The factory scans for classes with this annotation and registers them automatically. To add a
 * new transformer:
 *
 * <ol>
 *   <li>Implement {@link RecordTransformer}
 *   <li>Annotate with {@code @TransformerName("your-name")}
 *   <li>Set {@code record_transformer = 'your-name'} in {@code _table_config}
 * </ol>
 *
 * <p>Example:
 *
 * <pre>{@code
 * @TransformerName("spark")
 * public class SparkRecordTransformer implements RecordTransformer { ... }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TransformerName {
  /**
   * The transformer name used in {@code _table_config.record_transformer}. Case-insensitive during
   * lookup.
   */
  String value();
}
