/**
 * Rollup configuration for checking bundle size.
 * Run with: npm run checksize
 *
 * This bundles the library and reports:
 * - Original size
 * - Minified size
 * - Gzipped size
 * - Brotli size
 */

import {nodeResolve} from '@rollup/plugin-node-resolve';
import terser from '@rollup/plugin-terser';
import summary from 'rollup-plugin-summary';

const plugins = [
  nodeResolve(),
  terser({
    ecma: 2024,
    module: true,
    compress: {
      passes: 2,
      pure_getters: true,
    },
  }),
  summary({
    showMinifiedSize: true,
    showGzippedSize: true,
    showBrotliSize: true,
  }),
];

export default [
  {
    input: 'index.js',
    output: {
      file: '.checksize/index.js',
      format: 'es',
    },
    plugins,
  },
  {
    // Only expose/wrap — measures what a consumer who ignores newer barrel
    // exports (e.g. notify) actually pays.
    input: 'checksize-minimal.js',
    output: {
      file: '.checksize/minimal.js',
      format: 'es',
    },
    plugins,
  },
  {
    // The sequenced-channel opt-in on its own — it is a separate entry point,
    // so this is what importing it costs on top of the bundles above.
    input: 'handlers/channel.js',
    output: {
      file: '.checksize/channel.js',
      format: 'es',
    },
    plugins,
  },
];
