/**
 * Programmatic-advertising realtime pipeline.
 *
 * <p>Classes are grouped by pipeline layer and responsibility:</p>
 *
 * <ul>
 *   <li>{@code job}: Flink job entry point and pipeline orchestration</li>
 *   <li>{@code config}: runtime configuration</li>
 *   <li>{@code model}: records shared between pipeline layers</li>
 *   <li>{@code source}: Kafka/MySQL CDC source construction</li>
 *   <li>{@code dwd}: cleansing and attribution detail processing</li>
 *   <li>{@code dws}: windowed metric aggregation</li>
 *   <li>{@code sink}: Kafka and StarRocks output adapters</li>
 * </ul>
 */
package cn.edu.ustc.lakehouse.realtime;
