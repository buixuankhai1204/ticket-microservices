import swc from 'unplugin-swc';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [swc.vite()],
  test: {
    include: ['tests/**/*.spec.ts'],
    environment: 'node',
    fileParallelism: false,
    testTimeout: 30000,
    hookTimeout: 60000,
  },
});
