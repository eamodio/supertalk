/**
 * Minimal checksize entry point.
 *
 * Bundles only `expose`/`wrap`, so `npm run checksize` can report what a
 * consumer who ignores newer barrel exports (e.g. `notify`) actually pays,
 * alongside the full-barrel total.
 *
 * @fileoverview Minimal rollup input for bundle-size measurement.
 */
export {expose, wrap} from './index.js';
