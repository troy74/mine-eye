import { Canvas } from "@react-three/fiber";
import { OrbitControls } from "@react-three/drei";
import { TrajectoryView } from "./TrajectoryView";

type Props = { trajectoryUrl: string | null };

/**
 * Plain GridHelper avoids drei's infinite Grid shader (can crash some GPUs / WebGL stacks).
 */
function GroundGrid() {
  return (
    <group rotation={[Math.PI / 2, 0, 0]} position={[0, 0, 0]}>
      <gridHelper args={[400, 40, "#30363d", "#21262d"]} />
    </group>
  );
}

export function Scene({ trajectoryUrl }: Props) {
  return (
    <Canvas camera={{ position: [150, 150, 150], fov: 50 }}>
      <color attach="background" args={["#0f1419"]} />
      <ambientLight intensity={0.6} />
      <directionalLight position={[80, 120, 60]} intensity={0.8} />
      <GroundGrid />
      {trajectoryUrl ? <TrajectoryView url={trajectoryUrl} /> : null}
      <OrbitControls makeDefault />
    </Canvas>
  );
}
