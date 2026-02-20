#include "quant_hft/strategy/atomic_factory.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>

namespace quant_hft {
namespace {

class TestAtomicStrategy final : public IOpeningStrategy {
   public:
    void Init(const AtomicParams& params) override { (void)params; }
    std::string GetId() const override { return "test_atomic_strategy"; }
    void Reset() override {}

    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override {
        (void)state;
        (void)ctx;
        return {};
    }
};

class RegisteredAtomicStrategy final : public IOpeningStrategy {
   public:
    void Init(const AtomicParams& params) override { (void)params; }
    std::string GetId() const override { return "registered_atomic_strategy"; }
    void Reset() override {}

    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override {
        (void)state;
        (void)ctx;
        return {};
    }
};

QUANT_HFT_REGISTER_ATOMIC_STRATEGY("atomic_factory_registered_dummy", RegisteredAtomicStrategy);

std::string UniqueAtomicType() {
    static std::atomic<int> seq{0};
    return "atomic_factory_test_type_" + std::to_string(seq.fetch_add(1));
}

}  // namespace

TEST(AtomicFactoryTest, RegistersAndCreatesAtomicStrategy) {
    AtomicFactory factory;
    std::string error;
    const std::string type = UniqueAtomicType();
    ASSERT_TRUE(factory.Register(
        type, []() { return std::make_unique<TestAtomicStrategy>(); }, &error))
        << error;

    std::unique_ptr<IAtomicStrategy> strategy = factory.Create(type, &error, "opening-1");
    ASSERT_NE(strategy, nullptr) << error;
    EXPECT_EQ(strategy->GetId(), "test_atomic_strategy");
}

TEST(AtomicFactoryTest, RejectsDuplicateRegistration) {
    AtomicFactory factory;
    std::string error;
    const std::string type = UniqueAtomicType();
    ASSERT_TRUE(factory.Register(
        type, []() { return std::make_unique<TestAtomicStrategy>(); }, &error))
        << error;

    error.clear();
    EXPECT_FALSE(
        factory.Register(type, []() { return std::make_unique<TestAtomicStrategy>(); }, &error));
    EXPECT_NE(error.find("already registered"), std::string::npos);
}

TEST(AtomicFactoryTest, UnknownTypeErrorContainsIdAndType) {
    AtomicFactory factory;
    std::string error;

    std::unique_ptr<IAtomicStrategy> strategy =
        factory.Create("missing_type", &error, "unknown-opening");
    EXPECT_EQ(strategy, nullptr);
    EXPECT_NE(error.find("unknown-opening"), std::string::npos);
    EXPECT_NE(error.find("missing_type"), std::string::npos);
}

TEST(AtomicFactoryTest, MacroRegistrationRegistersIntoGlobalFactory) {
    std::string error;
    std::unique_ptr<IAtomicStrategy> strategy = AtomicFactory::Instance().Create(
        "atomic_factory_registered_dummy", &error, "macro-opening");
    ASSERT_NE(strategy, nullptr) << error;
    EXPECT_EQ(strategy->GetId(), "registered_atomic_strategy");
}

}  // namespace quant_hft
