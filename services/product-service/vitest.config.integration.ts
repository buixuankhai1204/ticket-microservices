import swc from 'unplugin-swc';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [swc.vite()],
  test: {
    include: ['tests/integration-*.spec.ts'],
    environment: 'node',
    fileParallelism: false,
    testTimeout: 120000,
    hookTimeout: 180000,
  },
});
