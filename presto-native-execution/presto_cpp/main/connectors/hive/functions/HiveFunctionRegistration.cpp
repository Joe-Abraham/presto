#include "presto_cpp/main/connectors/hive/functions/HiveFunctionRegistration.h"

#include "presto_cpp/main/connectors/hive/functions/InitcapFunction.h"
#include "presto_cpp/main/functions/dynamic_registry/CatalogFunctionRegistrar.h"
#include "presto_cpp/main/functions/dynamic_registry/DynamicFunctionRegistrar.h"

using namespace facebook::velox;
namespace facebook::presto::hive::functions {

namespace {
void registerHiveFunctions() {
  // Register functions under the 'hive.default' namespace.
  // This uses the catalog-based registration approach which allows
  // functions to be organized into catalogs with shared configuration.
  facebook::presto::registerCatalogFunction<InitCapFunction, Varchar, Varchar>(
      "hive", "default", "initcap");
}
} // namespace

void registerHiveNativeFunctions() {
  static std::once_flag once;
  std::call_once(once, []() { registerHiveFunctions(); });
}

} // namespace facebook::presto::hive::functions
