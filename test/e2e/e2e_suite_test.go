package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	k8slog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/env"
	testutils "sigs.k8s.io/gateway-api-inference-extension/test/utils"
)

const (
	// kindClusterName is the name of the Kind cluster created for e2e tests.
	kindClusterName = "e2e-tests"

	// redisManifest is the manifest for the Redis deployment and service.
	redisManifest = "./yaml/redis.yaml"
	// igwMockManifest is the manifest for the mock inference gateway.
	igwMockManifest = "./yaml/igw-mock.yaml"
	// promMockManifest is the manifest for the mock Prometheus server.
	promMockManifest = "./yaml/prom-mock.yaml"
	// asyncProcessorManifest is the manifest for the async-processor deployment.
	asyncProcessorManifest = "./yaml/async-processor.yaml"
	// asyncProcessorGcsManifest is the manifest for the async-processor deployment for gcs.
	asyncProcessorGcsManifest = "./yaml/async-processor-gcs.yaml"
	// asyncProcessorSaturationManifest is the manifest for the saturation-gated async-processor.
	asyncProcessorSaturationManifest = "./yaml/async-processor-saturation.yaml"
	// gcsMockManifest is the manifest for the GCS mock.
	gcsMockManifest = "./yaml/gcs-mock.yaml"
)

var (
	redisPort    string = env.GetEnvString("E2E_REDIS_PORT", "30379", ginkgo.GinkgoLogr)
	adminPort    string = env.GetEnvString("E2E_ADMIN_PORT", "30081", ginkgo.GinkgoLogr)
	promMockPort string = env.GetEnvString("E2E_PROM_MOCK_PORT", "30091", ginkgo.GinkgoLogr)
	gcsMockPort  string = env.GetEnvString("E2E_GCS_MOCK_PORT", "34443", ginkgo.GinkgoLogr)

	testConfig *testutils.TestConfig

	containerRuntime = env.GetEnvString("CONTAINER_TOOL", env.GetEnvString("CONTAINER_RUNTIME", "docker", ginkgo.GinkgoLogr), ginkgo.GinkgoLogr)
	apImage          = env.GetEnvString("AP_IMAGE", "ghcr.io/llm-d-incubation/async-processor:e2e-test", ginkgo.GinkgoLogr)
	igwMockImage     = "e2e-igw-mock:latest"
	promMockImage    = "e2e-prom-mock:latest"
	// nsName is the namespace in which the K8S objects will be created
	nsName = env.GetEnvString("NAMESPACE", "e2e-test", ginkgo.GinkgoLogr)

	redisObjects                    []string
	igwMockObjects                  []string
	promMockObjects                 []string
	gcsMockObjects                  []string
	asyncProcessorObjects           []string
	asyncProcessorGcsObjects        []string
	asyncProcessorSaturationObjects []string
	createdNameSpace                bool

	rdb         *redis.Client
	adminURL    string
	promMockURL string
	gcsMockURL  string
	gcsMockHost string
)

func TestEndToEnd(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t,
		"End To End Test Suite",
	)
}

var _ = ginkgo.BeforeSuite(func() {
	setupK8sCluster()
	testConfig = testutils.NewTestConfig(nsName, "")
	setupK8sClient()
	setupNameSpace()
	applyManifests()
	setupRedisClient()
	setupGCSClient()
})

var _ = ginkgo.AfterSuite(func() {
	if rdb != nil {
		rdb.Close() //nolint:errcheck
	}

	skipCleanup := env.GetEnvString("E2E_SKIP_CLEANUP", "false", ginkgo.GinkgoLogr)
	if skipCleanup == "true" {
		fmt.Println("Skipping cluster cleanup (E2E_SKIP_CLEANUP=true)")
		return
	}

	// delete kind cluster we created
	ginkgo.By("Deleting kind cluster " + kindClusterName)
	command := exec.Command("kind", "delete", "cluster", "--name", kindClusterName)
	session, err := gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	if err != nil {
		ginkgo.GinkgoLogr.Error(err, "Failed to delete kind cluster")
	} else {
		gomega.Eventually(session).WithTimeout(60 * time.Second).Should(gexec.Exit())
	}
})

// setupK8sCluster creates the Kind cluster, builds images, and loads them.
func setupK8sCluster() {
	// Check if cluster already exists
	checkCmd := exec.Command("kind", "get", "clusters")
	output, err := checkCmd.CombinedOutput()
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	clusterExists := false
	for _, cluster := range strings.Split(string(output), "\n") {
		if strings.TrimSpace(cluster) == kindClusterName {
			clusterExists = true
			break
		}
	}

	var command *exec.Cmd
	var session *gexec.Session

	if !clusterExists {
		ginkgo.By("Creating Kind cluster " + kindClusterName)
		command = exec.Command("kind", "create", "cluster", "--name", kindClusterName, "--wait", "120s", "--config", "-")
		stdin, err := command.StdinPipe()
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		go func() {
			defer func() {
				err := stdin.Close()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			}()
			clusterConfig := strings.ReplaceAll(kindClusterConfig, "${REDIS_PORT}", redisPort)
			clusterConfig = strings.ReplaceAll(clusterConfig, "${ADMIN_PORT}", adminPort)
			clusterConfig = strings.ReplaceAll(clusterConfig, "${PROM_MOCK_PORT}", promMockPort)
			clusterConfig = strings.ReplaceAll(clusterConfig, "${GCS_MOCK_PORT}", gcsMockPort)
			_, err := io.WriteString(stdin, clusterConfig)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		}()
		session, err = gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))
	} else {
		ginkgo.By("Kind cluster " + kindClusterName + " already exists, skipping creation")
	}

	ginkgo.By("Building async-processor image")
	command = exec.Command(containerRuntime, "build", "-t", apImage, projectRoot())
	session, err = gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))

	ginkgo.By("Building igw-mock image")
	command = exec.Command(containerRuntime, "build", "-t", igwMockImage,
		filepath.Join(projectRoot(), "test", "e2e", "igw-mock"))
	session, err = gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))

	ginkgo.By("Building prom-mock image")
	command = exec.Command(containerRuntime, "build", "-t", promMockImage,
		filepath.Join(projectRoot(), "test", "e2e", "prom-mock"))
	session, err = gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))

	kindLoadImage(apImage)
	kindLoadImage(igwMockImage)
	kindLoadImage(promMockImage)

	// Pull and load redis image
	ginkgo.By("Pulling redis:7-alpine")
	command = exec.Command(containerRuntime, "pull", "redis:7-alpine")
	session, err = gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))
	kindLoadImage("redis:7-alpine")

	// Pull and load fake-gcs-server image
	ginkgo.By("Pulling fsouza/fake-gcs-server:latest")
	command = exec.Command(containerRuntime, "pull", "fsouza/fake-gcs-server:latest")
	session, err = gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))
	kindLoadImage("fsouza/fake-gcs-server:latest")
}

func kindLoadImage(image string) {
	ginkgo.By(fmt.Sprintf("Loading %s into the cluster %s", image, kindClusterName))

	command := exec.Command("kind", "load", "docker-image", image, "--name", kindClusterName)
	session, err := gexec.Start(command, ginkgo.GinkgoWriter, ginkgo.GinkgoWriter)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	gomega.Eventually(session).WithTimeout(600 * time.Second).Should(gexec.Exit(0))
}

func setupK8sClient() {
	k8sCfg, err := config.GetConfigWithContext("")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.ExpectWithOffset(1, k8sCfg).NotTo(gomega.BeNil())

	err = clientgoscheme.AddToScheme(testConfig.Scheme)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	testConfig.CreateCli()

	k8slog.SetLogger(ginkgo.GinkgoLogr)
}

// setupNameSpace sets up the specified namespace if it doesn't exist
func setupNameSpace() {
	ginkgo.By("Cleaning up ClusterRoleBindings if they exist")
	for _, crb := range []string{"async-processor-auth-delegator", "async-processor-saturation-auth-delegator"} {
		err := testConfig.KubeCli.RbacV1().ClusterRoleBindings().Delete(testConfig.Context, crb, metav1.DeleteOptions{})
		if err != nil && !errors.IsNotFound(err) {
			ginkgo.GinkgoLogr.Error(err, "Failed to delete ClusterRoleBinding", "name", crb)
		}
	}

	err := testConfig.KubeCli.CoreV1().Namespaces().Delete(testConfig.Context, nsName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	if err == nil {
		ginkgo.By("Waiting for namespace " + nsName + " to be deleted")
		gomega.Eventually(func() bool {
			_, err := testConfig.KubeCli.CoreV1().Namespaces().Get(testConfig.Context, nsName, metav1.GetOptions{})
			return errors.IsNotFound(err)
		}, 60*time.Second, 1*time.Second).Should(gomega.BeTrue())
	}

	ginkgo.By("Creating namespace " + nsName)
	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: nsName,
		},
	}
	_, err = testConfig.KubeCli.CoreV1().Namespaces().Create(testConfig.Context, namespace, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	createdNameSpace = true
}

func applyManifests() {
	ginkgo.By("Applying Redis manifest")
	redisObjects = testutils.ApplyYAMLFile(testConfig, redisManifest)

	ginkgo.By("Applying igw-mock manifest")
	igwMockObjects = testutils.ApplyYAMLFile(testConfig, igwMockManifest)

	ginkgo.By("Applying prom-mock manifest")
	promMockObjects = testutils.ApplyYAMLFile(testConfig, promMockManifest)

	ginkgo.By("Applying gcs-mock manifest")
	gcsMockObjects = testutils.ApplyYAMLFile(testConfig, gcsMockManifest)

	ginkgo.By("Applying async-processor manifest")
	apYamls := testutils.ReadYaml(asyncProcessorManifest)
	apYamls = substituteMany(apYamls, map[string]string{
		"${AP_IMAGE}": apImage,
	})
	asyncProcessorObjects = testutils.CreateObjsFromYaml(testConfig, apYamls)

	ginkgo.By("Applying async-processor-gcs manifest")
	apGcsYamls := testutils.ReadYaml(asyncProcessorGcsManifest)
	apGcsYamls = substituteMany(apGcsYamls, map[string]string{
		"${AP_IMAGE}": apImage,
	})
	asyncProcessorGcsObjects = testutils.CreateObjsFromYaml(testConfig, apGcsYamls)

	ginkgo.By("Applying async-processor-saturation manifest")
	apSatYamls := testutils.ReadYaml(asyncProcessorSaturationManifest)
	apSatYamls = substituteMany(apSatYamls, map[string]string{
		"${AP_IMAGE}": apImage,
	})
	asyncProcessorSaturationObjects = testutils.CreateObjsFromYaml(testConfig, apSatYamls)
}

func setupRedisClient() {
	adminURL = "http://localhost:" + adminPort
	promMockURL = "http://localhost:" + promMockPort
	gcsMockURL = "http://localhost:" + gcsMockPort
	gcsMockHost = "localhost:" + gcsMockPort

	ginkgo.By("Creating Redis client on localhost:" + redisPort)
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:" + redisPort,
	})
	gomega.Eventually(func() error {
		return rdb.Ping(context.Background()).Err()
	}, 30*time.Second, 1*time.Second).Should(gomega.Succeed())

	ginkgo.By("Waiting for prom-mock to be ready on localhost:" + promMockPort)
	gomega.Eventually(func() error {
		resp, err := http.Get(promMockURL + "/admin/saturation")
		if err != nil {
			return err
		}
		return resp.Body.Close()
	}, 30*time.Second, 1*time.Second).Should(gomega.Succeed())

	ginkgo.By("Waiting for gcs-mock to be ready on localhost:" + gcsMockPort)
	gomega.Eventually(func() error {
		resp, err := http.Get(gcsMockURL + "/_internal/healthcheck")
		if err != nil {
			return err
		}
		return resp.Body.Close()
	}, 30*time.Second, 1*time.Second).Should(gomega.Succeed())
}

// projectRoot returns the root of the project (two levels up from test/e2e/).
func projectRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..")
}

func substituteMany(inputs []string, substitutions map[string]string) []string {
	outputs := []string{}
	for _, input := range inputs {
		output := input
		for key, value := range substitutions {
			output = strings.ReplaceAll(output, key, value)
		}
		outputs = append(outputs, output)
	}
	return outputs
}

const kindClusterConfig = `
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- extraPortMappings:
  - containerPort: 30379
    hostPort: ${REDIS_PORT}
    protocol: TCP
  - containerPort: 30081
    hostPort: ${ADMIN_PORT}
    protocol: TCP
  - containerPort: 30091
    hostPort: ${PROM_MOCK_PORT}
    protocol: TCP
  - containerPort: 30443
    hostPort: ${GCS_MOCK_PORT}
    protocol: TCP
`
